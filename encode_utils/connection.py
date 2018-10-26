# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

import base64
import json
import logging
import mimetypes
import os
import re
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import subprocess
import sys
import urllib

# inhouse libraries
import encode_utils.transfer_to_gcp
import encode_utils as eu
import encode_utils.profiles as eup
import encode_utils.utils as euu


#: The directory that contains the log files created by the `Connection` class.
LOG_DIR = "EU_Logs"

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AwardPropertyMissing(Exception):
    """
    Raised when the `award` property isn't set in the payload when doing a POST, and a default isn't
    set by the environment variable `DCC_AWARD` either.
    """
    message = ("The property '{}' is missing from the payload and a default isn't set either. To"
               " store a default, set the DCC_AWARD environment variable.")


class FileUploadFailed(Exception):
    """
    Raised when the AWS CLI returns a non-zero exit status.
    """

class MissingAlias(Exception):
    """
    Raised when POSTING a payload that doesn't contain the 'aliases' property and the argument
    require_aliases in Connection.post() is set to False.
    """

class LabPropertyMissing(Exception):
    """
    Raised when the `lab` property isn't set in the payload when doing a POST, and a default isn't
    set by the environment variable `DCC_LAB` either.
    """
    message = ("The property '{}' is missing from the payload and a default isn't set either. To"
               " store a default, set the DCC_LAB environment variable.")


class ProfileNotSpecified(Exception):
    """
    Raised when the profile (object schema) to submit to isn't specifed in a POST payload.
    """
    pass


class RecordIdNotPresent(Exception):
    pass


class RecordNotFound(Exception):
    """
    Raised when a record that should exist on the Portal can't be retrieved via a GET request.
    """
    pass

class S3ToGCPFailed(Exception):
    """
    Raised when a file from an ENCODE S3 bucket fails to copy to a GCP bucket path.
    """
    pass


class Connection():
    """Handles communication with the Portal regarding data submission and retrieval.

    For data submission or modification, and working with non-released datasets, you must have
    the environment variables `DCC_API_KEY` and `DCC_SECRET_KEY` set. Check with your DCC data wrangler
    if you haven't been assigned these keys.

    There are three log files opened in append mode in the directory specified by ``connection.LOG_DIR`` that
    are specific to whichever Portal you are connected to. When connected to Production, each log file
    name will include the token '_prod_'. For Development, the token will be '_dev_'. The three
    log files are named accordingly in reference to their purpose, and are classified as:

    1. debug log file - All messages sent to STDOUT are also written to this log file. In addition,
       all messages written to the error log file described below are logged here.
    2. error log file - Only terse error messages are sent to this log file for quick scanning of
       any potential issues. If you identify an error here that needs more explanation, then you
       should consult the debug log file.
    3. posted log file - Tabulates what was successfully POSTED. There are three tab-delimited
       colummns ordered as submission timestamp, record alias, and record accession (or UUID if
       the `accession` property doesn't exist for the profile of the record at hand). Note that if a
       record has several aliases, then only the first one in the list for the `aliases` property is
       used.
    """

    #: Identifies the name of the key in the payload that stores a valid ENCODE-assigned
    #: identifier for a record, such as alias, accession, uuid, md5sum, ... depending on
    #: the object being submitted.
    #: This is not a valid property of any ENCODE object schema, and is used in the ``patch()``
    #: instance method to designate the record to update.
    ENCID_KEY = "_enc_id"

    #: Identifies the name of the key in the payload that stores the ID of the profile
    #: to submit to. Like ``ENCID_KEY``, this is a non-schematic key that is used only internally.
    PROFILE_KEY = "_profile"

    #: Constant
    POST = "post"
    #: Constant
    PATCH = "patch"

    def __init__(self, dcc_mode=None, dry_run=False, submission=False):

        #: A reference to the `debug` logging instance that was created earlier in ``encode_utils.debug_logger``.
        #: This class adds a file handler, such that all messages sent to it are logged to this
        #: file in addition to STDOUT.
        self.debug_logger = logging.getLogger(eu.DEBUG_LOGGER_NAME)

        # Be sure to set self.dcc_mode before creating the logging file handlers since the mode is
        # used as part of the file name.

        #: An indication of which Portal instance to use. Set to 'prod' for the production Portal,
        #: and 'dev' for the development Portal. Alternatively, you can set an explicit host, such as
        #: demo.encodedcc.org. Leaving the default of None means to use the value of the `DCC_MODE`
        #: environment variable.
        self.dcc_mode = self._set_dcc_mode(dcc_mode)
        self.dcc_host = eu.DCC_MODES[self.dcc_mode]["host"]
        self.dcc_url = eu.DCC_MODES[self.dcc_mode]["url"]

        #: Set to True to prevent any server-side changes on the ENCODE Portal, i.e. PUT, POST,
        #: PATCH, DELETE requests will not be sent to the Portal. After-POST and after-PATCH
        #: hooks (see the instance method :meth:`after_submit_hooks`) will not be run either in
        #: this case. You can turn off this dry-run feature by calling the instance method
        #: :meth:`set_live_run`.
        self.dry_run = dry_run

        # Add debug file handler to debug_logger:
        self._add_file_handler(logger=self.debug_logger, level=logging.DEBUG, tag="debug")

        #: A ``logging`` instance with a file handler for logging terse error messages.
        #: The log file resides locally within the directory specified by the constant
        #: ``connection.LOG_DIR``. Accepts messages >= ``logging.ERROR``.
        self.error_logger = logging.getLogger(eu.ERROR_LOGGER_NAME)
        log_level = logging.ERROR
        self.error_logger.setLevel(log_level)
        self._add_file_handler(logger=self.error_logger, level=log_level, tag="error")
        self.log_error("Connecting to {}".format(self.dcc_host))

        #: A ``logging`` instance with a file handler for logging successful POST operations.
        #: The log file resides locally within the directory specified by the constant
        #: ``connection.LOG_DIR``. Accepts messages >= ``logging.INFO``.
        self.post_logger = logging.getLogger(eu.POST_LOGGER_NAME)
        log_level = logging.INFO
        self.post_logger.setLevel(log_level)
        self._add_file_handler(logger=self.post_logger, level=log_level, tag="posted")

        self.check_dry_run() #If on, signal this in the logs.

        #: Indicates whether this class is being use to submit objects to the Portal. The main
        #: effect of setting this option to True is to update the default behavior of the
        #: ``self.get()`` method, such that it it fetches its payload through the database directly
        #: rather than any index. That is useful when you are submitting several inter-dependent
        #: objects in turn and the new objects haven't yet had time to be indexed (otherwise you risk
        #: getting a 404 response back meaning "Resource Not Found". This attribute can be also set
        #: via the instance method ``self.set_submission``.
        self.set_submission(submission) #sets self.submission attribute.

        #: The API key to use when authenticating with the DCC servers. This is set automatically
        #: to the value of the `DCC_API_KEY` environment variable in the ``_set_api_keys()`` private
        #: instance method.
        self.api_key = self._set_api_keys()[0]
        #: The secret key to use when authenticating with the DCC servers. This is set automatically
        #: to the value of the `DCC_SECRET_KEY` environment variable in the ``_set_api_keys()`` private
        #: instance method.
        self.secret_key = self._set_api_keys()[1]
        if self.api_key and self.secret_key:
            self.auth = (self.api_key, self.secret_key)
        else:
            self.auth = ()
            self.log_error(
                "WARNING: API keys {} not set, all functions have no permission".format(
                    self.auth))

    def _set_dcc_mode(self, dcc_mode=False):
        if not dcc_mode:
            try:
                dcc_mode = os.environ["DCC_MODE"]
                self.debug_logger.debug("Utilizing DCC_MODE environment variable.")
            except KeyError:
                print("ERROR: You must supply the `dcc_mode` argument or set the environment variable DCC_MODE.")
                sys.exit(-1)
        dcc_mode = dcc_mode.lower()
        if dcc_mode not in eu.DCC_MODES:
            # Assume dcc_mode is a valid demo host.
            url = 'https://' + dcc_mode + '/'
            try:
                requests.get(url, timeout=2)
            except requests.exceptions.ConnectionError:
                print(
                    "ERROR: The specified dcc_mode of '{}' is not valid. Should be one of '{}' or a valid demo.encodedcc.org hostname.".format(
                        dcc_mode,
                        list(
                            eu.DCC_MODES.keys())))
                sys.exit(-1)

            eu.DCC_MODES[dcc_mode] = {
                'host': dcc_mode,
                'url': url
            }
        return dcc_mode

    def _get_logfile_name(self, tag):
        """
        Creates a name for a log file that is meant to be used in a call to
        ``logging.FileHandler``. The log file name will incldue the path to the log directory given
        by the `LOG_DIR` constant. The format of the file name is: 'log_$HOST_$TAG.txt', where
        $HOST is the hostname part of the URL given by ``self.URL``, and $TAG is the value of the
        'tag' argument. The log directory will be created if need be.

        Args:
            tag: `str`. A tag name to add to at the end of the log file name for clarity on the
                log file's purpose.
        """
        if not os.path.exists(LOG_DIR):
            os.mkdir(LOG_DIR)
        filename = "log_eu_" + self.dcc_mode + "_" + tag + ".txt"
        filename = os.path.join(LOG_DIR, filename)
        return filename

    def _add_file_handler(self, logger, level, tag):
        """
        Adds a ``logging.FileHandler`` handler to the specified ``logging`` instance that will log
        the messages it receives at the specified error level or greater.  The log file name will
        be of the form log_$HOST_$TAG.txt, where $HOST is the hostname part of the URL given
        by ``self.URL``, and $TAG is the value of the 'tag' argument.

        Args:
            logger: The `logging.Logger` instance to add the `logging.FileHandler` to.
            level:  `int`. A logging level (i.e. given by one of the constants `logging.DEBUG`,
                `logging.INFO`, `logging.WARNING`, `logging.ERROR`, `logging.CRITICAL`).
            tag: `str`. A tag name to add to at the end of the log file name for clarity on the
                log file's purpose.
        """
        f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')
        filename = self._get_logfile_name(tag)
        handler = logging.FileHandler(filename=filename, mode="a")
        handler.setLevel(level)
        handler.setFormatter(f_formatter)
        logger.addHandler(handler)

    def _set_api_keys(self):
        """
        Retrieves the API key and secret key based on the environment variables `DCC_API_KEY` and
        `DCC_SECRET_KEY`.

        Returns:
            `tuple`: Two item tuple containing the API Key and the Secret Key
        """
        api_key = os.environ.get("DCC_API_KEY")
        secret_key = os.environ.get("DCC_SECRET_KEY")
        return api_key, secret_key

    def _log_post(self, aliases, dcc_id):
        """
        Uses the self.post_logger to log a newly POSTED object's aliases and dcc_id. 
        Each message is written in a three column format delimited by a tab character. The columns are:
          1) primary alias: The first alias appearing in the provided 'aliases' list argument.
          2) secondary aliases: Any additional aliases appearing in the provided 'aliases' list argument.
                 These will be comma-delimited.
          2) DCC identifier: The value of the dcc_id argument. 

        Note that it is possible that the aliases list is empty, in which case only the dcc_id will
        be present in the last column of the written line. 

        Args:
            aliases: `list`. The value of the 'aliases' key in the payload for the record that
                was POSTED.
            dcc_id: `str`. An ENCODE-generated identifier on the ENCODE Portal for the new record
                that was POSTED, i.e. accession, uuid, md5sum.
        """
        try:
            primary = aliases[0]
        except KeyError:
            primary = ""
        try:
            secondary = aliases[1:]
        except KeyError:
            secondary = []
        entry = primary + "\t" + ",".join(secondary) + "\t" + dcc_id
        self.post_logger.info(entry)

    def set_submission(self, status):
        """Sets the boolean value of the ``self.submission`` attribute.

        Args:
            status: `bool`.
        """
        self.submission = status
        if self.submission:
            self.debug_logger.debug("submission=True: In submission mode.")
        else:
            self.debug_logger.debug("submission=False: In non-submission mode.")

    def check_dry_run(self):
        """
        Checks if the dry-run feature is enabled, and if so, logs the fact. This is mainly meant to
        be called by other methods that are designed to make modifications on the ENCODE Portal.

        Returns:
            `True`: The dry-run feature is enabled.
            `False`: The dry-run feature is turned off.
        """
        if self.dry_run:
            self.log_error("DRY RUN is enabled.")
            return True
        return False

    def set_dry_run(self):
        """Enables the dry-run feature and logs the fact."""
        self.dry_run = True
        self.log_error("DRY RUN is enabled")

    def set_live_run(self):
        """Disables the dry-run feature and logs the fact."""
        self.dry_run = False
        self.log_error("DRY RUN is disabled.")

    def log_error(self, msg):
        """Sends 'msg' to both ``self.error_logger`` and ``self.debug_logger``.
        """
        self.debug_logger.debug(msg)
        self.error_logger.error(msg)

    def get_aliases(self, dcc_id, strip_alias_prefix=False):
        """
        Given an ENCODE identifier for an object, performs a GET request and extracts the aliases.

        Args:
            dcc_id: `str`. The ENCODE ID for a given object, i.e ENCSR999EHG.
            strip_alias_prefix: `bool`. `True` means to remove the alias prefix if all return aliases.

        Returns:
            `list`: The aliases.
        """
        record = self.get(ignore404=False, rec_ids=dcc_id)
        aliases = record[eu.ALIAS_PROP_NAME]
        for index in range(len(aliases)):
            alias = aliases[index]
            if strip_alias_prefix:
                aliases[index] = euu.strip_alias_prefix(alias)
        return aliases

    def indexing(self):
        """Indicates whether the Portal is updating its schematic indicies.

        Returns:
            `bool`: True if the Portal is indexing, False otherwise.

        """
        response = self.get("_indexer", ignore404=False)
        status = response["status"]
        if status == "indexing":
            return True
        return False

    def make_search_url(self, search_args):
        """Creates a URL encoded URL given the search arguments.

        Args:
            search_args: `list` of two-item tuples of the form ``[(key, val), (key, val), ...]``.

        Returns:
            `str`: The URL containing the URL encoded query.
        """
        # urllib doesn't contain the parse() method until you import urllib3 (weird, but that's what I noticed).
        query = urllib.parse.urlencode(search_args)
        url = os.path.join(self.dcc_url, "search/?") + query
        return url

    def search(self, search_args=[], url=None, limit=None):
        """
        Searches the Portal using the provided query parameters, which will first be URL encoded.
        The user can pass in the query parameters and values via the `search_args` argument, or
        pass in a URL directly that contains a query string via the `url` argument, or provide
        values for both arguments in which case the query parameters specified in `search_args` will
        be added to the query parameters given in the URL.

        Args:
            search_args: `list` of two-item tuples of the form ``[(key, val), (key, val) ,...]``.
                To support a != style query, append "!" to the key name.
            url: `str`. A URL used to search for records interactively in the ENCODE Portal. The
                query will be extracted from the URL.
            limit: `int`. The number of search results to send from the server. The default means
                to return all results.

        Returns:
            `list`: The search results.

        Raises:
            `requests.exceptions.HTTPError`: The status code is not ok and != 404.
        """
        if url:
            # Format query string into list of tuples:
            url_obj = urllib.parse.urlsplit(url)
            query_list = urllib.parse.parse_qsl(url_obj.query)
            # Ex: If the query string is originally
            #
            # ?type=Experiment&assay_title=ChIP-seq&award.rfa=ENCODE4&lab.title=Michael+Snyder%2C+Stanford&status=in+progress&status=submitted"
            #
            # then query_list looks like this:
            #
            # [('assay_title', 'ChIP-seq'), ('award.rfa', 'ENCODE4'), ('lab.title', 'Michael Snyder, Stanford'), ('status', 'in progress'), ('status', 'submitted'), ('type', 'Experiment')]
            #
            # Convert query_list into a dict. Note that I could have used urllib.parse.parse_qs
            # above instead of urllib.parse.parse_qsl, in which case it would look like this:
            #
            # {'type': ['Biosample'], 'lab.title': ['Michael Snyder, Stanford'], 'award.rfa': ['ENCODE4'], 'biosample_type!': ['tissue']}
            #
            # but that causes problems when calling urllib.parse.urlencode, since the list literals
            # become url encoded too.
            #
            # Merge the search_args dict into the query_list, overwriting values in query_list
            # if same keys are present:
            if search_args:
                query_list.extend(search_args)
        else:
            query_list = search_args
        query_list = sorted(query_list)
        params = [x[0] for x in query_list]
        if "limit" not in params:
            if not limit:
                query_list.append(("limit", "all"))
            else:
                query_list.append(("limit", str(limit)))

        url = self.make_search_url(search_args=query_list)
        self.debug_logger.debug("Searching DCC with query {url}.".format(url=url))
        response = requests.get(url,
                                auth=self.auth,
                                timeout=eu.TIMEOUT,
                                headers=euu.REQUEST_HEADERS_JSON,
                                verify=False)
        status_code = response.status_code
        if not response.ok and status_code != requests.codes.NOT_FOUND:
            response.raise_for_status()
        return response.json()["@graph"]  # the @graph object is a list

    def get_profile_from_payload(self, payload):
        """
        Useful to call when doing a POST (and ``self.post()`` does call this). Ensures that the profile key
        identified by ``self.PROFILE_KEY`` exists in the passed-in payload and that the value is
        a recognized ENCODE object profile (schema) identifier. Alternatively, the user can set the profile in
        the more convoluted `@id` property.

        Args:
            payload: `dict`. The intended object data to POST.

        Returns:
            `str`: The ID of the profile if all validations pass, otherwise.

        Raises:
            encode_utils.connection.ProfileNotSpecified: Both keys ``self.PROFILE_KEY`` and `@id` are
              missing in the payload.
            encode_utils.profiles.UnknownProfile: The profile ID isn't recognized by the class
                `encode_utils.profiles.Profile`.
        """

        profile_id = payload.get(self.PROFILE_KEY)
        if not profile_id:
            profile_id = payload.get("@id")
            if not profile_id:
                raise ProfileNotSpecified(
                    ("You need to specify the ID of the profile to submit to by using the '{}' key"
                     " in the payload, or by setting the `@id` property explicitely.").format(self.PROFILE_KEY))
        profile = eup.Profile(profile_id)  # raises euu.UnknownProfile if unknown profile ID.
        return profile.profile_id

    def get_lookup_ids_from_payload(self, payload):
        """
        Given a payload to submit to the Portal, extracts the identifiers that can be used to lookup
        the record on the Portal, i.e. to see if the record already exists. Identifiers are extracted
        from the following fields:

        1. ``self.ENCID_KEY``,
        2. aliases,
        3. md5sum (in the case of a file object)

        Args:
            payload: `dict`. The data to submit.

        Returns:
            `list`: The possible lookup identifiers.
        """
        lookup_ids = []
        if self.ENCID_KEY in payload:
            lookup_ids.append(payload[self.ENCID_KEY])
        if eu.ALIAS_PROP_NAME in payload:
            lookup_ids.extend(payload[eu.ALIAS_PROP_NAME])
        if "md5sum" in payload:
            # The case for file objects.
            lookup_ids.append(payload["md5sum"])

        lookup_ids = [x.strip() for x in lookup_ids]
        lookup_ids = [x for x in lookup_ids]
        if not lookup_ids:
            raise RecordIdNotPresent(
                ("The payload does not contain a recognized identifier for traceability. For example,"
                 " you need to set the 'aliases' key, or specify an ENCODE assigned identifier in the"
                 " non-schematic key {}.".format(self.ENCID_KEY)))

        return lookup_ids

    # def delete(self,rec_id):
    #    """Not supported at present by the DCC - Only wranglers can delete objects.
    #    """
    #    url = os.path.join(self.dcc_url,rec_id)
    #    self.logger.info(
    #      (">>>>>>DELETING {rec_id} From DCC with URL {url}").format(rec_id=rec_id,url=url))
    #    if self.dry_run:
    #        return {}
    #    response = requests.delete(url,auth=self.auth,timeout=eu.TIMEOUT,headers=euu.REQUEST_HEADERS_JSON, verify=False)
    #    if response.ok:
    #        return response.json()
    #    response.raise_for_status()

    def get(self, rec_ids, database=False, ignore404=True, frame=None):
        """GET a record from the Portal.

        Looks up a record in the Portal and performs a GET request, returning the JSON serialization of
        the object. You supply a list of identifiers for a specific record, and the Portal will be
        searched for each identifier in turn until one is either found or the list is exhausted.

        Args:
            rec_ids: `str` or `list`. Must be a `list` if you want to supply more than one identifier.
                For a few example identifiers, you can use a uuid, accession, ..., or even the value of
                a record's `@id` property.
            database: `bool`. If True, then search the database directly instead of the Elasticsearch.
                 indices. Always True when in submission mode (`self.submission` is True).
            frame: `str`. A value for the frame query parameter, i.e. 'object', 'edit'. See
                https://www.encodeproject.org/help/rest-api/ for details.
            ignore404: `bool`. Only matters when none of the passed in record IDs were found on the
                Portal.  In this case, If set to `True`, then an empty `dict` will be returned.
                If set to `False`, then an Exception will be raised.


        Returns:
            `dict`: The JSON response. Will be empty if no record was found AND ``ignore404=True``.

        Raises:
            `Exception`: If the server responds with a FORBIDDEN status.
            `requests.exceptions.HTTPError`: The status code is not ok, and the
                cause isn't due to a 404 (not found) status code when ``ignore404=True``.
        """
        if self.submission:
            database = True
        if isinstance(rec_ids, str):
            rec_ids = [rec_ids]
        status_codes = {}  # key is return code, value is the record ID
        for r in rec_ids:
            r = r.strip("/")
            url = os.path.join(self.dcc_url, r, "?format=json")
            if database:
                url += "&datastore=database"
            if frame:
                url += "&frame={frame}".format(frame=frame)
            self.debug_logger.debug(">>>>>>GET {rec_id} From DCC with URL {url}".format(
                rec_id=r, url=url))
            response = requests.get(url,
                                    auth=self.auth,
                                    timeout=eu.TIMEOUT,
                                    headers=euu.REQUEST_HEADERS_JSON,
                                    verify=False)
            if response.ok:
                return response.json()
            status_codes[response.status_code] = r

        if requests.codes.FORBIDDEN in status_codes:
            raise Exception(
                "Access to ENCODE record {} is forbidden".format(status_codes[requests.codes.FORBIDDEN]))
        elif requests.codes.NOT_FOUND in status_codes:
            self.debug_logger.debug("NOT FOUND")
            if ignore404:
                return {}
        # At this point in the code, the response is not okay.
        # Raise the error for last response we got:
        response.raise_for_status()

    def set_attachment(self, document):
        """
        Sets the `attachment` property for any profile that supports it, such as `document` or
        `antibody_characterization`.

        Args:
            document: `str`. A local file path.

        Returns:
            `dict`. The 'attachment' propery value.
        """
        download_filename = os.path.basename(document)
        mime_type = mimetypes.guess_type(download_filename)[0]
        data = base64.b64encode(open(document, 'rb').read())
        temp_uri = str(data, "utf-8")
        href = "data:{mime_type};base64,{temp_uri}".format(mime_type=mime_type, temp_uri=temp_uri)
        #download_filename = library_alias.split(":")[1] + "_relative_knockdown.jpeg"
        attachment = {}
        attachment["download"] = download_filename
        attachment["type"] = mime_type
        attachment["href"] = href
        return attachment

    def after_submit_file_cloud_upload(self, rec_id, profile_id):
        """An after-POST submit hook for uploading files to AWS.

        Some objects, such as Files (`file.json` profile) need to have a corresponding file in the cloud.
        Where in the cloud the actual file should be uploaded to is indicated in File object's
        `file.upload_credentials.upload_url` property. Once the File object is posted, this hook is
        used to perform the actual cloud upload of the physical, local file represented by the File object.

        Args:
            rec_id: `str`. An identifier for the new File object on the Portal.
            profile_id: `str`. The ID of the profile that the record belongs to.
        """
        if profile_id != eup.Profile.FILE_PROFILE_ID:
            return
        self.upload_file(file_id=rec_id)

    def after_submit_hooks(self, rec_id, profile_id, method=""):
        """
        Calls after-POST and after-PATCH hooks. This method is called from both the ``post()`` and
        ``patch()`` instance methods. Returns the None object immediately if the dry-run feature
        is enabled.

        Some hooks only run if you are doing a PATCH, others if you are only doing a POST. Then there
        are some that run if you are doing either operation. Each hook that is called
        can potentially modify the payload.

        Args:
            rec_id: `str`. An identifier for the record on the Portal.
            profile_id: `str`. The profile identifier indicating the profile that the record belongs to.
            method: str. One of ``self.POST`` or ``self.PATCH``, or the empty string to indicate which
                registered hooks to look through.
        """
        if self.check_dry_run():
            return
        # Check allowed_methods. Will matter later when there are POST-specific
        # and PATCH-specific hooks.
        allowed_methods = [self.POST, self.PATCH, ""]
        if not method in allowed_methods:
            raise Exception(
                "Unknown method '{}': must be one of {}.".format(
                    method, allowed_methods))

        # Call agnostic hooks
        #... None yet.

        # Call POST-specific hooks if POST:
        if method == self.POST:
            self.after_submit_file_cloud_upload(rec_id, profile_id)

        # Call PATCH-specific hooks if PATCH:
        #... None yet.

    def before_submit_alias(self, payload):
        """
        A pre-POST and pre-PATCH hook used to 
          1) Clean alias names by removing disallowed characters indicated by the DCC schema for
             the alias property. 
          2) Add the lab alias prefix to any aliases that are missing it. 
             The `DCC_LAB` environment variable is consulted to fetch the lab name, and if not
             set then this will be a no-op.

        Args:
            payload: `dict`. The payload to submit to the Portal.

        Returns:
            `dict`: The potentially modified payload.
        """
        if not eu.ALIAS_PROP_NAME in payload:
            return payload
        aliases = euu.clean_aliases(payload[eu.ALIAS_PROP_NAME])
        aliases = euu.add_alias_prefix(aliases)
        payload[eu.ALIAS_PROP_NAME] = aliases
        return payload

    def before_submit_attachment(self, payload):
        """
        A pre-POST and pre-PATCH hook used to simplify the creation of an attachment in profiles
        that support it.

        Checks the payload for the presence of the `attachment` property that is used by certain
        profiles, i.e. `document` and `antibody_characterization`, and then checks to see if a particular
        shortcut is being employed to indicate the attachment. That shortcut works as follows: if the
        dictionary value of the 'attachment' key has a key named 'path' in it (case-sensitive), then
        the value is taken to be the path to a local file. Then, the actual attachment object is
        constructed, as defined in the `document` profile, by calling ``self.set_attachment()``.  Note that
        this shortcut is particular to this ``Connection`` class, and when used the 'path' key should be
        the only key in the attachment dictionary as any others will be ignored.

        Args:
            payload: `dict`. The payload to submit to the Portal.

        Returns:
            `dict`: The potentially modified payload.
        """
        attachment_prop = "attachment"
        path = "path"

        if attachment_prop in payload:
            val = payload[attachment_prop]  # dict
            if path in val:
                # Then set the actual attachment object:
                attachment = self.set_attachment(document=val[path])
                payload[attachment_prop] = attachment
        return payload

    def before_post_file(self, payload):
        """A pre-POST hook that calculates and sets the `md5sum` property for a file record.

        If the 'md5sum' key is already present in the payload, then this is a no-op.

        Args:
            payload: `dict`. The payload to submit to the Portal.

        Returns:
            `dict`: The potentially modified payload.

        Raises:
            encode_utils.utils.MD5SumError: Perculated through the function
              `encode_utils.utils.calculate_md5sum` when it can't calculate the md5sum.
        """
        profile_id = payload[self.PROFILE_KEY]
        if profile_id != eup.Profile.FILE_PROFILE_ID:
            return payload
        try:
            file_name = payload[eup.Profile.SUBMITTED_FILE_PROP_NAME]
        except KeyError:
            return payload
        if eup.Profile.MD5SUM_NAME_PROP_NAME in payload:
            if payload[eup.Profile.MD5SUM_NAME_PROP_NAME]:
                # Already set; nothing to do.
                return payload
        md5sum = euu.calculate_md5sum(file_name)
        payload["md5sum"] = md5sum
        return payload

    def before_post_fastq_file(self, payload):
        """
        A pre-POST hook for FASTQ file objects that checks whether certain rules are followed as
        defined in the file.json schema.

        For example, if the FASTQ file is sequenced single-end, then the property ``File.run_type``
        should be set to `single-ended` as expected, however, the property ``File.paired_end``
        shouldn't be set in the payload, as the ``File.run_type`` property has the commment:

          Only paired-ended files should have paired_end values

        """
        profile_id = payload[self.PROFILE_KEY]
        if profile_id != eup.Profile.FILE_PROFILE_ID:
            return payload

        run_type = payload.get("run_type")
        if not run_type:
            return payload

        if run_type == "single-ended":
            if "paired_end" in payload:
                payload.pop("paired_end")
        return payload

    def before_submit_hooks(self, payload, method=""):
        """Calls pre-POST and pre-PATCH hooks. This method is called from both the ``post()`` and
        ``patch()`` instance methods.

        Some hooks only run if you are doing a PATCH, others if you are only doing a POST. Then there
        are some that run if you are doing either operation. Each hook that is called
        can potentially modify the payload.

        Args:
            payload: `dict`. The payload to POST or PATCH.
            method: `str`. One of "post" or "patch", or the empty string to indicate which registered
                hooks to call. Some hooks are agnostic to the HTTP method, and these hooks are
                always called. Setting `method` to the empty string means to only call these
                agnostic hooks.

        Returns:
            `dict`: The potentially modified payload that has been passed through all applicable
            pre-submit hooks.
        """
        # Check allowed_methods. Will matter later when there are POST-specific
        # and PATCH-specific hooks.
        allowed_methods = [self.POST, self.PATCH, ""]
        if not method in allowed_methods:
            raise Exception(
                "Unknown method '{}': must be one of {}.".format(
                    method, allowed_methods))

        # Call agnostic hooks
        payload = self.before_submit_attachment(payload)
        payload = self.before_submit_alias(payload)

        # Call POST-specific hooks if POST:
        if method == self.POST:
            payload = self.before_post_file(payload)
            payload = self.before_post_fastq_file(payload)

        # Call PATCH-specific hooks if PATCH:
        #... None yet.

        return payload

    def post(self, payload, require_aliases=True):
        """POST a record to the Portal.

        Requires that you include in the payload the non-schematic key ``self.PROFILE_KEY`` to
        designate the name of the ENCODE object profile that you are submitting to, or the
        actual `@id` property itself.

        If the `lab` property isn't present in the payload, then the default will be set to the value
        of the `DCC_LAB` environment variable. Similarly, if the `award` property isn't present, then the
        default will be set to the value of the `DCC_AWARD` environment variable.

        Before the POST is attempted, any pre-POST hooks are fist called (see the method
        ``self.before_submit_hooks``).

        Args:
            payload: `dict`. The data to submit.
            require_aliases: `bool`.  `True` means that the 'aliases' property is to be required in
                 `payload`. This is the default and it is highly recommended not to change this
                 because it'll be easy to create duplicates on the server if accidentally POSTING
                 the same payload again.  For example, you can easily create the same biosample
                 as many times as you want on the Portal when not providing an alias.  Furthermore,
                 submitting labs should include at least one alias per record being submitted
                 to the Portal for traceabilty purposes in the submitting lab.

        Returns:
            `dict`: The JSON response from the POST operation, or the existing record if it already
            exists on the Portal (where a GET on any of it's aliases, when provided in the payload,
            finds the existing record).

        Raises:
            encode_utils.connection.AwardPropertyMissing: The `award` property isn't present in the payload and there isn't a
                defualt set by the environment variable `DCC_AWARD`.
            encode_utils.connection.LabPropertyMissing: The `lab` property isn't present in the payload and there isn't a
                default set by the environment variable `DCC_LAB`.
            encode_utils.connection.MissingAlias: The argument 'require_aliases' is set to True and
                the 'aliases' property is missing in the payload or is empty.
            encode_utils.connection.requests.exceptions.HTTPError: The return status is not ok.

        Side effects:
            self.PROFILE_KEY will be popped out of the payload if present, otherwise, the key "@id"
            will be popped out. Furthermore, self.ENCID_KEY will be popped out if present in the payload.
        """
        self.debug_logger.debug("\nIN post().")
        # Make sure we have a payload that can be converted to valid JSON, and
        # tuples become arrays, ...
        payload = json.loads(json.dumps(payload))
        profile_id = self.get_profile_from_payload(payload)
        payload[self.PROFILE_KEY] = profile_id
        url = os.path.join(self.dcc_url, profile_id)
        if self.ENCID_KEY in payload:
            # Shouldn't be here, unless maybe a PATCH was attempted and the record didn't exist, so
            # a POST was then attempted.
            payload.pop(self.ENCID_KEY)
        # Check if we need to add defaults for 'award' and 'lab' properties:
        if profile_id not in eup.Profile.AWARDLESS_PROFILE_IDS:  # No lab prop for these profiles either.
            if eu.AWARD_PROP_NAME not in payload:
                if not eu.AWARD:
                    raise AwardPropertyMissing
                payload.update(eu.AWARD)
            if eu.LAB_PROP_NAME not in payload:
                if not eu.LAB:
                    raise LabPropertyMissing
                payload.update(eu.LAB)

        # Run 'before' hooks:
        payload = self.before_submit_hooks(payload, method=self.POST)
        # Remove the non-schematic self.PROFILE_KEY if being used, which was added above since some
        # 'before' hooks may need it. Also check for the `@id` property and remove it too if found.
        try:
            payload.pop(self.PROFILE_KEY)
        except KeyError:
            pass
        try:
            payload.pop("@id")
        except KeyError:
            pass

        no_alias = False #Use this to check later if doing a GET
        aliases = payload.get(eu.ALIAS_PROP_NAME)
        if not aliases:
            if profile_id in eup.Profile.NO_ALIAS_PROFILE_IDS or not require_aliases:
                aliases = ["N/A"]
                no_alias = True
            else:
                raise MissingAlias(
                    ("Missing property '{}' in payload {}. This is required by default for the profiles"
                     " that include this property, and can be disabled by setting the `require_aliases`"
                     " argument to False in the call to this method, being `encode_utils.connection.Connection.post()`").format(eu.ALIAS_PROP_NAME,payload))

        # Validate the payload against the schema
        ### This doesn't work as locally I can't use jsonschema to validate a profile with
        ### custom objects specified in the value of a linkTo property.
        #self.debug_logger.debug("Validating the payload against the schema")
        #validation_error = euu.err_context(payload=payload, schema=eup.Profile.PROFILES[profile_id])
        #if validation_error:
        #    self.log_error("Invalid schema instance of the {} profile.".format(profile_id))
        #    self.debug_logger.debug("Payload is: {}".format(euu.print_format_dict(payload)))
        #    self.log_error(validation_error[0]) # The top-level validation message
        #    if validation_error[1]: # The validation context can be empty
        #        self.debug_logger.debug(euu.print_format_dict(validation_error[1]))
        #    raise Exception(euu.print_format_dict(validation_error[0]))

        self.debug_logger.debug(
            ("<<<<<< POST {alias} To DCC with URL {url} and this"
             " payload:\n\n{payload}\n\n").format(alias=aliases[0], url=url, payload=euu.print_format_dict(payload)))

        if self.check_dry_run():
            return {}
        response = requests.post(url,
                                 auth=self.auth,
                                 timeout=eu.TIMEOUT,
                                 headers=euu.REQUEST_HEADERS_JSON,
                                 json=payload,
                                 verify=False)
        #response_json = response.json()["@graph"][0]
        response_json = response.json()

        if response.ok:
            self.debug_logger.debug("Success.")
            response_json = response_json["@graph"][0]
            encid = ""
            try:
                encid = response_json["accession"]
            except KeyError:
                # Some objects don't have an accession, i.e. replicates.
                encid = response_json["uuid"]
            self._log_post(aliases=aliases, dcc_id=encid)
            # Run 'after' hooks:
            self.after_submit_hooks(encid, profile_id, method=self.POST)
            return response_json
        elif response.status_code == requests.codes.CONFLICT:
            self.debug_logger.debug(response_json)
            # In the case of paired-end FASTQ files, it could also mean that there was a conflict
            # related to the 'paired_with' property, i.e. the latter is already linked to a FASTQ
            # file, which could even have been set to a deleted state on the Portal. The server
            # response in either case would look something like this:
            #
            # {
            #   'detail': "Keys conflict: [('file:paired_with', 'f39320d9-0970-4369-b680-5965a5e85b6f')]",
            #   'description': 'There was a conflict when trying to complete your request.',
            #   'code': 409,
            #   '@type': ['HTTPConflict', 'Error'],
            #   'title': 'Conflict',
            #   'status': 'error'}
            # }
            #
            if no_alias:
                response.raise_for_status()
            else:
                existing_record = self.get(rec_ids=aliases, ignore404=True)
                if not existing_record:
                    response.raise_for_status()
                else:
                    self.log_error("Will not POST '{}' since it already exists with aliases '{}'.".format(aliases[0], existing_record["aliases"]))
                    return existing_record

        else:
            message = "Failed to POST {}".format(aliases[0])
            self.log_error(message)
            self.debug_logger.debug("<<<<<< DCC POST RESPONSE: ")
            self.debug_logger.debug(euu.print_format_dict(response_json))
            response.raise_for_status()

    def patch(self, payload, raise_403=True, extend_array_values=True):
        """PATCH a record on the Portal.

        Before the PATCH is attempted, any pre-PATCH hooks are fist called (see the method
        ``self.before_submit_hooks()``). If the PATCH fails due to the resource not being found (404),
        then that fact is logged to both the debug and error loggers.

        Args:
            payload: `dict`. containing the attribute key and value pairs to patch. Must contain the key
                ``self.ENCID_KEY`` in order to indicate which record to PATCH.
            raise_403: `bool`. `True` means to raise a ``requests.exceptions.HTTPError`` if a 403 status
                (forbidden) is returned.
                If set to `False` and there still is a 403 return status, then the object you were
                trying to PATCH will be fetched from the Portal in JSON format as this function's
                return value.
            extend_array_values: `bool`. Only affects keys with array values. `True` (default) means to
                extend the corresponding value on the Portal with what's specified in the payload.
                `False` means to replace the value on the Portal with what's in the payload.

        Returns:
            `dict`: The JSON response from the PATCH operation, or an empty dict if the record doesn't
                    exist on the Portal. Will also be an empty dict if the dry-run feature is enabled.

        Raises:
            KeyError: The payload doesn't have the key ``self.ENCID_KEY`` set AND there aren't
                any aliases provided in the payload's 'aliases' key.
            requests.exceptions.HTTPError: if the return status is not ok (excluding a
                403 status if 'raise_403' is False.
        """
        # Make sure we have a payload that can be converted to valid JSON, and
        # tuples become arrays, ...
        payload = json.loads(json.dumps(payload))
        self.debug_logger.debug("\nIN patch()")
        encode_id = payload[self.ENCID_KEY]
        # Ensure that the record exists on the Portal:
        rec_json = self.get(rec_ids=encode_id, frame="edit", ignore404=True)
        if not rec_json:
            return {}

        if extend_array_values:
            for key in payload:
                if isinstance(payload[key], list):
                    val = payload[key]
                    val.extend(rec_json.get(key, []))
                    # I use rec_json.get(key,[]) above because in a GET request,
                    # not all props are pulled back when they are empty.
                    # For ex, in a file object, if the controlled_by prop isn't set, then
                    # it won't be in the response.
                    payload[key] = list(set(val))

        # Run 'before' hooks:
        payload = self.before_submit_hooks(payload, method=self.PATCH)
        payload.pop(self.ENCID_KEY)
        if self.PROFILE_KEY in payload:
            # Some client software may add this key in; won't hurt to remove it.
            payload.pop(self.PROFILE_KEY)

        url = os.path.join(self.dcc_url, encode_id.lstrip("/"))
        self.debug_logger.debug(
            ("<<<<<< PATCHING {encode_id} To DCC with URL"
             " {url} and this payload:\n\n{payload}\n\n").format(
                 encode_id=encode_id, url=url, payload=euu.print_format_dict(payload)))

        if self.check_dry_run():
            return {}
        response = requests.patch(url, auth=self.auth, timeout=eu.TIMEOUT, headers=euu.REQUEST_HEADERS_JSON,
                                  json=payload, verify=False)
        response_json = response.json()

        if response.ok:
            self.debug_logger.debug("Success.")
            response_json = response_json["@graph"][0]
            uuid = response_json["uuid"]
            profile_id = eup.Profile(response_json["@id"]).profile_id
            # Run 'after' hooks:
            self.after_submit_hooks(uuid, profile_id, method=self.PATCH)
            return response_json
        elif response.status_code == requests.codes.FORBIDDEN:
            # Don't have permission to PATCH this object.
            if not raise_403:
                return rec_json

        message = "Failed to PATCH {}".format(encode_id)
        self.log_error(message)
        self.debug_logger.debug("<<<<<< DCC PATCH RESPONSE: ")
        self.debug_logger.debug(euu.print_format_dict(response_json))
        response.raise_for_status()

    def remove_props(self, rec_id, props=[]):
        """Runs a PUT request to remove properties of interest on the specified record.

        Note that before-submit and after-submit hooks are not run here as they would be in
        `self.path()` or `self.post()` (:meth:`before_submit_hooks` and :meth:`after_submit_hooks`
        are not called).

        Args:
            rec_id: `str`. An identifier for the record on the Portal.
            props: `list`. The properties to remove from the record.

        Raises:

        Returns:
            `dict`. Contains the JSON returned from the PUT request.

        """
        self.debug_logger.debug("\nIN remove_props()")
        rec_json = self.get(rec_ids=rec_id, frame="object", ignore404=False)
        profile = eup.Profile(rec_json["@id"])
        del rec_json
        editable_json = self.get(rec_ids=rec_id, frame="edit", ignore404=False)
        # For good house-keeping, check for any props that we definitely aren't allowed to remove,
        # and raise an Exception if one is present in the supplied 'props' list. Some properties,
        # such as accession, submitted_by, ..., still show up in a GET with 'frame="edit"', and
        # the Portal will most likely complain or silently disallow an attempt to remove such
        # properites. Nonetheless, a well-behaved client shouldn't send uncouth requests, so some
        # checking is performed below for good measure:
        for prop in props:
            if profile.is_prop_required(prop):
                raise Exception("Can't remove required property")
            elif profile.is_prop_not_submittable(prop):
                raise Exception("Can't remove non-submittable property.")
            elif profile.is_prop_read_only(prop):
                raise Exception("Can't remove read-only property.")
            else:
                # Then it is safe to remove this property.
                editable_json.pop(prop)

        url = os.path.join(self.dcc_url, rec_id)
        self.debug_logger.debug("Attempting to remove properties {} from record '{}' by sending a PUT request with payload {}.".format(props, rec_id, euu.print_format_dict(editable_json)))
        if self.check_dry_run():
            return
        response = requests.put(
            url,
            auth=self.auth,
            timeout=eu.TIMEOUT,
            headers=euu.REQUEST_HEADERS_JSON,
            json=editable_json,
            verify=False
        )
        response.raise_for_status()
        self.debug_logger.debug("Success")
        response_json = response.json()
        return response_json


    def send(self, payload, error_if_not_found=False, extend_array_values=True, raise_403=True):
        """
        .. deprecated:: 1.1.1
           Will be removed in the next major release.

        A wrapper over ``self.post()`` and ``self.patch()`` that determines which to call based on whether the
        record exists on the Portal.  Especially useful when submitting a high-level object,
        such as an experiment which contains many dependent objects, in which case you could have a mix
        where some need to be POST'd and some PATCH'd.

        Args:
            payload: `dict`. The data to submit.
            error_if_not_found: `bool`. If set to `True`, then a PATCH will be attempted and a
                ``requests.exceptions.HTTPError`` will be raised if the record doesn't exist on the Portal.
            extend_array_values: `bool`. Only matters when doing a PATCH, and Only affects keys with
                array values. `True` (default) means to extend the corresponding value on the Portal
                with what's specified in the payload. `False` means to replace the value on the Portal
                with what's in the payload.
            raise_403: `bool`. Only matters when doing a PATCH. `True` means to raise an
                requests.exceptions.HTTPError if a 403 status (forbidden) is returned.
                If set to `False` and there still is a 403 return status, then the object you were
                trying to PATCH will be fetched from the Portal in JSON format as this function's
                return value (as handled by ``self.patch()``).

        Raises:
              requests.exceptions.HTTPError: You want to do a PATCH (indicated by setting
                  ``error_if_not_found=True``) but the record isn't found.
        """
        # Check wither record already exists on the portal
        self.debug_logger.debug("WARNING: Connection.send() is deprecated since v1.1.1.")
        lookup_ids = self.get_lookup_ids_from_payload(payload)
        rec_json = self.get(rec_ids=lookup_ids, ignore404=not error_if_not_found)

        if not rec_json:
            return self.post(payload=payload)
        else:
            # PATCH
            if self.ENCID_KEY not in payload:
                encode_id = aliases[0]
                payload[self.ENCID_KEY] = encode_id
            return self.patch(
                payload=payload, extend_array_values=extend_array_values, raise_403=raise_403)

    def get_fastqfiles_on_exp(self, exp_id):
        """Returns a list of all FASTQ file objects in the experiment.

        Args:
            exp_id: `str`. An Experiment identifier.

        Returns:
            `list`: Each element is the JSON form of a FASTQ file record.
        """
        fastq_records_json = []
        exp_json = self.get(exp_id, ignore404=False)
        files = exp_json["files"]
        for file_json in files:
            if file_json["file_type"] != "fastq":
                continue  # this is not a file object for a FASTQ file.
            fastq_records_json.append(file_json)
        return fastq_records_json

    def get_fastqfile_replicate_hash(self, exp_id):
        """
        Given a DCC experiment ID, gets its JSON representation from the Portal and looks in the
        `original` property to find FASTQ file objects and creates a `dict` organized by replicate
        numbers. Keying through the `dict` by replicate numbers, you can get to a particular file
        object's JSON serialization.

        Args:
            exp_id: `str`. An Experiment identifier.
        Returns:
            `dict`: `dict` where each key is a biological_replicate_number.
            The value of each key is another `dict` where each key is a technical_replicate_number.
            The value of this is yet another `dict` with keys being file read numbers -
            1 for forward reads, 2 for reverse reads.  The value
            for a given key of this most inner dictionary is a list of JSON-serialized file objects.
        """
        fastq_file_records = self.get_fastqfiles_on_exp(exp_id)
        dico = {}
        for file_json in fastq_file_records:
            brn = file_json["replicate"]["biological_replicate_number"]
            trn = file_json["replicate"]["technical_replicate_number"]

            try:
                read_num = int(file_json["paired_end"])  # string
            except KeyError:
                # File.paired_end property not included when File.run_type="single-ended".
                read_num = 1

            if brn not in dico:
                dico[brn] = {}
            if trn not in dico[brn]:
                dico[brn][trn] = {}
            if read_num not in dico[brn][trn]:
                dico[brn][trn][read_num] = []
            dico[brn][trn][read_num].append(file_json)
        return dico

    def extract_aws_upload_credentials(self, creds):
        """
        Sets values for the AWS CLI security credentials (for uploading a file to AWS S3) to the
        credentials found in a file record's `upload_credentials` property. The security credentials
        are stored in a `dict` where the keys are named after environment variables to be used by
        the AWS CLI.

        Args:
            creds: `dict`: The value of a File object's `upload_credentials` property.

        Returns:
            `dict`: `dict` containing keys named after AWS CLI environment variables being:

              1. AWS_ACCESS_KEY_ID,
              2. AWS_SECRET_ACCESS_KEY,
              3. AWS_SECURITY_TOKEN,
              4. UPLOAD_URL

            Will be empty if the `upload_credentials` property isn't present in `file_json`.
        """
        aws_creds = {}
        aws_creds["AWS_ACCESS_KEY_ID"] = creds["access_key"]
        aws_creds["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
        aws_creds["AWS_SESSION_TOKEN"] = creds["session_token"]
        aws_creds["UPLOAD_URL"] = creds["upload_url"]
        return aws_creds

    def get_upload_credentials(self, file_id):
        """
        Similar to ``self.extract_aws_upload_credentials()``, but it goes a step further in that it is
        capable of regenerating the upload credentials if they aren't currently present in the file
        record.

        Args:
            file_id: `str`. A file object identifier (i.e. accession, uuid, alias, md5sum).

        Returns:
            `dict`: The value of the `upload_credentials` property if present, otherwise, the `dict`
             returned by ``self.regenerate_aws_upload_creds``, which tries to generate the value for
             this property.
        """
        # Be sure to set database=True so that the database is searched instead of Elasticsearch, as
        # the latter doesn't store the upload_credentials. Must also set frame="object" for this
        # to work.
        file_json = self.get(file_id, frame="object", database=True, ignore404=False)
        try:
            creds = file_json["upload_credentials"]
        except KeyError:
            creds = self.regenerate_aws_upload_creds(file_id)
            # Will be None if forbidden.

        # URL example from dev Portal:
        #  s3://encoded-files-dev/2018/01/28/7c5c6d58-c98a-48b4-9d4b-3296b4126b89/TSTFF334203.fastq.gz"
        #  That's the uuid after the date.
        return creds

    def regenerate_aws_upload_creds(self, file_id):
        """Reissues AWS S3 upload credentials for the specified file record.

        Args:
            file_id: `str`. An identifier for a file record on the Portal.

        Returns:
            `dict`: `dict` containing the value of the 'upload_credentials' key in the JSON serialization
            of the file record represented by `file_id`. Will be empty if new upload credentials
            could not be issued.

        Raises:
            `requests.exceptions.HTTPError`: The response from the server isn't a successful status code.
        """
        self.debug_logger.debug("Attempting to generate new file upload credentials")
        # Don't use curl since it
        #   1) requires that all users have it installed, and
        #   2) only works for the most recent versions when interacting with the ENCODE servers.

#        cmd = ("curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json'"
#               " https://{api_key}:{secret_key}@{host}/files/{file_id}/upload -d '{{}}'"
#               " | python3 -m json.tool").format(api_key=self.api_key, secret_key=self.secret_key, host=self.dcc_host, file_id=file_id)

        response = requests.post(
            os.path.join(self.dcc_url, "files", file_id, "@@upload"),
            auth=self.auth,
            headers=euu.REQUEST_HEADERS_JSON,
            json = {},
            timeout=eu.TIMEOUT)
        response_json = response.json()
        if response.ok:
            self.debug_logger.debug("Success: upload credentials for '{}' regenerated.".format(file_id))
            upload_creds = response_json["@graph"][0]["upload_credentials"]
            return upload_creds
        else:
            status_code = response.status_code
            err_msg = "Error {}: unable to re-issue upload credentials for '{}'".format(status_code, file_id)
            self.log_error(err_msg)
            self.debug_logger.debug(euu.print_format_dict(response_json))
            response.raise_for_status()

            # For ex: response would look like this for a 404.

            # {
            #     "@type": [
            #         "HTTPNotFound",
            #         "Error"
            #     ],
            #     "code": 404,
            #     "description": "The resource could not be found.",
            #     "detail": "/files/michael-snyder:test_file_1/upload",
            #     "status": "error",
            #     "title": "Not Found"
            # }

            # You get a 403 when the 'status' of the file object isn't set to 'uploading'.
            # You also get this when the file object no-longer has read access (was
            # archived by wranglers).

        #Don't log the full response as it contains sensative security information.

    def gcp_transfer_urllist(self, file_ids, filename):
        """
        Creates a "URL list" file to be used by the Google Storage Transfer Service (STS); see documentation at 
        https://cloud.google.com/storage-transfer/docs/create-url-list. Once the URL list is created,
        you need to upload it somewhere that Google STS can reach it via HTTP or HTTPS. I recommend
        uploading the URL list to your GCS bucket. From there, you can get an HTTPS URL for it by 
        clicking on your file name (while in the GCP Console) and then copying the URL shown in your 
        Web browser, which can in turn be pasted directly in the Google STS.

        Args:
            file_ids: `list` of file identifiers. The corresponding S3 objects must have public read
                permission as required for the URL list.
            filename: `str`. The output filename in TSV format, which can be fed into the Google STS.
        """
        fout = open(filename, 'w')
        fout.write("TsvHttpData-1.0\n")
        for i in file_ids:
            url = self.s3_object_path(rec_id=i, url=True)
            # One with DCC API keys can get the URL in a more straightforward manner by doing a GET on
            # the files @@upload endpoint. But this even requires AWS keys even when the file in 
            # question is released. For broader community support, the above workaround is in use.
            rec = self.get(i, ignore404=False)
            md5 = base64.b64encode(bytes.fromhex(rec["md5sum"]))
            fout.write("\t".join([url, str(rec["file_size"]), md5.decode("utf-8")]) + "\n")
        fout.close()

    def gcp_transfer_from_aws(self, file_ids, gcp_bucket, gcp_project, description="", aws_creds=()):
        """
        Copies one or more ENCODE files from AWS S3 storage to GCP storage by using the Google STS.
        This is similar to the :meth:`gcp_transfer_urllist` method - the difference is that S3 object
        paths are copied directly instead of using public HTTPS URIs, and AWS keys are required here. 

        See :func:`encode_utils.transfer_to_gcp.Transfer` for full documentation.

        Args:
            file_ids: `list`. One or more ENCODE files to transfer. They can be any valid ENCODE File
                object identifier. Don't mix ENCODE files from across buckets.
            gcp_bucket: `str`. The name of the GCP bucket.
            gcp_project: `str`. The GCP project that is associated with gcp_bucket. Can be given
                in either integer form  or the user-friendly name form (i.e. sigma-night-206802)
            description: `str`. The description to show when querying transfers via the
                 Google Storage Transfer API, or via the GCP Console. May be left empty, in which
                 case the default description will be the value of the first S3 file name to transfer.
            aws_creds: `tuple`. Ideally, your AWS credentials will be stored in the environment.
                For additional flexability though, you can specify them here as well in the form
                ``(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)``.

        Returns:
            `dict`: The JSON response representing the newly created transferJob.
        """
        s3_paths = []
        for i in file_ids:
            s3_paths.append(self.s3_object_path(rec_id=i))
        t = encode_utils.transfer_to_gcp.Transfer(gcp_project=gcp_project, aws_creds=aws_creds)
        # Figure out the s3 bucket by looking at the first s3 object. All specified s3 files should
        # from the same bucket.
        s3_bucket = s3_paths[0].split("/")[2]
        transfer_job = t.from_s3(s3_bucket=s3_bucket, s3_paths=s3_paths, gcp_bucket=gcp_bucket, description=description)
        return transfer_job


#    def gsutil_copy_file_to_gcp(self, s3obj, gcp_dest, aws_creds=()):
#        """
#        Uses gsutil.
#
#        Args:
#            s3obj: `str`. The S3 object to copy to GCP. Needs to be the full object path in the S3 bucket.
#            gcp_dest: `str`. The GCP bucket (including any path information) to copy the S3 object to.
#            aws_creds: `tuple` containing the AWS_ACCESS_KEY_ID, followed by the AWS_SECRET_ACCESS_KEY.
#                       The default is to fetch those values from environment variables.
#                       Unfortunatly, GCP doesn't support AWS pre-signed URLs, but when it does this
#                       method will accept a third item in the tuple, being the AWS_SESSION_TOKEN.
#        """
#        cmd = "gsutil cp '{}' '{}'".format(s3obj, gcp_dest)
#        self.debug_logger.debug("Running command '{}'.".format(cmd))
#        if self.check_dry_run():
#            return
#        environ = os.environ
#        if aws_creds:
#            environ.update(aws_creds)
#
#        popen = subprocess.Popen(cmd,
#                                 shell=True,
#                                 env=environ,
#                                 stdout=subprocess.PIPE,
#                                 stderr=subprocess.PIPE)
#        stdout, stderr = popen.communicate()
#        stdout = stdout.decode("utf-8")
#        stderr = stderr.decode("utf-8")
#        retcode = popen.returncode
#        if retcode:
#            error_msg = "Copy to GCP failed."
#            self.log_error(error_msg)
#            error_msg = (" Subprocess command '{cmd}' failed with return code '{retcode}'."
#                         " Stdout is '{stdout}'.  Stderr is '{stderr}'.").format(
#                cmd=cmd, retcode=retcode, stdout=stdout, stderr=stderr)
#            self.debug_logger.debug(error_msg)
#            raise S3ToGCPFailed(error_msg)
#        self.debug_logger.debug("Copy to GCP successful.")

    def upload_file(self, file_id, file_path=None, set_md5sum=False):
        """
        Uses the AWS CLI to upload a file to the Portal for the indicated file record. The file
        to upload can be specified in one of the following ways;

          1. Path to a local file,
          2. S3 object, or
          3. Google Storage object (Not yet supported; see ticket at https://github.com/GoogleCloudPlatform/gsutil/issues/535)

        For the last option listed, the user must have gsutil insalled with credentials configured (
        see https://github.com/StanfordBioinformatics/encode_utils/wiki/could-to-cloud-file-transfers
        for more details).

        If the dry-run feature is enabled, then this method will return prior to launching the
        upload command.

        Args:
            file_id: `str`. An identifier of a `file` record on the ENCODE Portal.
            file_path: `str`. The local path to the file to upload, or an S3 object (i.e s3://mybucket/test.txt),
              or a Google Storage object (i.e. gs://mybucket/test.txt).
              If not set, defaults to `None` in which case the local file path will be extracted from the
              record's `submitted_file_name` property.
            set_md5sum: `bool`. True means to also calculate the md5sum and set the file record's md5sum
              property on the Portal (this currently is only implemented for local files, not S3).
              This will always take place whenever the property isn't yet set and when uploading a
              local file.

        Raises:
            encode_utils.connection.FileUploadFailed: The return code of the AWS upload command was non-zero.
        """
        self.debug_logger.debug("\nIN upload_file()\n")
        #upload_credentials = self.get_upload_credentials(file_id) # Don't use this - they may have expired.
        upload_credentials = self.regenerate_aws_upload_creds(file_id)
        aws_creds = self.extract_aws_upload_credentials(upload_credentials)
        file_rec = self.get(rec_ids=file_id,ignore404=False)
        if not file_path:
            try:
                file_path = file_rec[eup.Profile.SUBMITTED_FILE_PROP_NAME]
            except KeyError:  # submitted_file_name property not set:
                raise Exception("No file path specified.")
        file_rec_md5sum = file_rec.get("md5sum")
        if not file_rec_md5sum or set_md5sum:
            if not file_path.startswith("s3"):
                # md5sum calc. supported at present only for local files.
                self.debug_logger.debug("Calculating md5sum for {}".format(os.path.basename(file_path)))
                md5sum = euu.calculate_md5sum(file_path)
                self.patch({self.ENCID_KEY: file_rec["@id"], "md5sum": md5sum})

        cmd_args = "{file_path} {upload_url}".format(file_path=file_path, upload_url=aws_creds["UPLOAD_URL"])
        if file_path.startswith("gs://"):
            cmd = "gsutil cp"
        else:
            cmd = "aws s3 cp"
        cmd += " " + cmd_args
        self.debug_logger.debug("Running command '{cmd}'.".format(cmd=cmd))
        if self.check_dry_run():
            return
        popen = subprocess.Popen(cmd,
                                 shell=True,
                                 env=os.environ.update(aws_creds),
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        stdout, stderr = popen.communicate()
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        retcode = popen.returncode
        if retcode:
            error_msg = "Failed to upload file '{}' for {}.".format(file_path, file_id)
            self.log_error(error_msg)
            error_msg = (" Subprocess command '{cmd}' failed with return code '{retcode}'."
                         " Stdout is '{stdout}'.  Stderr is '{stderr}'.").format(
                cmd=cmd, retcode=retcode, stdout=stdout, stderr=stderr)
            self.debug_logger.debug(error_msg)
            raise FileUploadFailed(error_msg)
        self.debug_logger.debug("AWS upload successful.")

    def get_platforms_on_experiment(self, rec_id):
        """
        Looks at all FASTQ files on the specified experiment, and tallies up the varying sequencing
        platforms that generated them.  The platform of a given file record is indicated by the
        `platform` property. This is moreless used to verify that there aren't a mix of
        multiple different platforms present as normally all reads should come from the same platform.

        Args:
            rec_id: `str`. DCC identifier for an experiment.
        Returns:
            `list`: The de-duplicated list of platforms seen on the experiment's FASTQ files.
        """
        fastq_files = self.get_fastqfiles_on_exp(rec_id)
        platforms = []
        for fastq_json in fastq_files:
            platforms.extend(fastq_json["platform"][eu.ALIAS_PROP_NAME])
        return list(set(platforms))

    def post_document(self, document, document_type, description):
        """POSTS a document to the Portal.

        The alias for the document will be the lab prefix plus the file name. The lab prefix is taken
        as the value of the `DCC_LAB` environment variable, i.e. 'michael-snyder'.

        Args:
            document_type: `str`. For possible values, see
              https://www.encodeproject.org/profiles/document.json. It appears that one should use
              "data QA" for analysis results documents.
            description: `str`. The description for the document.
            document: `str`. Local file path to the document to be submitted.

        Returns:
            `str`: The DCC UUID of the new document.
        """
        document_filename = os.path.basename(document)
        document_alias = eu.LAB[eu.LAB_PROP_NAME] + ":" + document_filename
        mime_type = mimetypes.guess_type(document_filename)[0]
        if not mime_type:
            raise Exception("Couldn't guess MIME type for {}.".format(document_filename))

        # Post information
        payload = {}
        payload[self.PROFILE_KEY] = "document"
        payload[eu.ALIAS_PROP_NAME] = [document_alias]
        payload["document_type"] = document_type
        payload["description"] = description

        attachment = self.set_attachment(document)

        payload['attachment'] = attachment

        response = self.post(payload=payload)
        return response['uuid']

    def download(self, rec_id, get_stream=False, directory=None):
        """
        Downloads the contents of the specified file or document object from the ENCODE Portal to
        either the calling directory or the indicated download directory. The downloaded file will
        be named as it is on the Portal.

        Args:

           rec_id: `str`. A DCC identifier for a file or document record on the Portal.
           directory: `str`. The full path to the directory in which to download the file. If not
               specified, then the file will be downloaded in the calling directory.

        Returns:
            `str`. The full path to the downloaded file.
        """
        rec = self.get(rec_id, ignore404=False)
        # Check whether we need to download a Document or File record.
        rec_type = rec["@type"]
        if "Document" in rec_type:
            file_type = False
            # There is a bug on the ENCODE Portal where setting the auth results in a 400 status
            # since documents are using some other type of authorization protocol.
            auth = ()
        elif "File" in rec_type:
            file_type = True
            auth = self.auth
        else:
            raise Exception("This method can only download records of type 'File' and 'Document'; '{}' is neither of these.".format(rec_id))
        # Formulate download URL:
        if file_type:
            url = os.path.join(self.dcc_url, rec["href"].lstrip("/"))
        else:
            url = os.path.join(self.dcc_url, "documents", rec["uuid"], rec["attachment"]["href"])
        r = requests.get(
            url,
            auth=auth,
            stream = True,
            timeout=eu.TIMEOUT,
            verify=False)
        r.raise_for_status()
        content_length = r.headers.get("Content-Length")
        self.debug_logger.debug("GET file {} from URL {}.".format(rec_id, url))
        if content_length:
            self.debug_logger.debug("File size: {:,.0f} bytes.".format(int(content_length)))
        if file_type:
            filename = r.headers["Content-Disposition"].split("filename=")[-1]
        else:
            filename = rec["attachment"]["download"]
        if directory:
            filename = os.path.join(directory,filename)
        fout = open(filename, "wb")
        if get_stream:
            return r
        # Download in chunks of 512 bytes
        for line in r.iter_content(chunk_size=512):
            fout.write(line)
        fout.close()
        self.debug_logger.debug("Download complete: {}.".format(filename))
        return filename

    def s3_object_path(self, rec_id, url=False):
        """
        Given an ENCODE File object's id (such as accession, uuid, alias), returns the full S3 object
        URI, or HTTP/HTTPS URI if url=True. 

        Args:
            rec_id: `str`. A DCC object identifier of the record to link the document to.
            url: `bool`. True means to return the HTTP/HTTPS URI of the file rather than the S3 URI.
                 Useful if this is a released file since you can download via the URL.
        """
        response = self.download(rec_id=rec_id, get_stream=True)
        redirect_url = response.url
        # i.e. redirect_url is
        # https://download.encodeproject.org/https://encode-files.s3.amazonaws.com/2017/05/12/4ae28
        # cf4-c0a7-409f-8d8d-384ba692096a/ENCFF985JCJ.bigWig?response-content-disposition=attachment%3B%2 ...
        url_obj = urllib.parse.urlsplit(redirect_url)
        url_path = url_obj.path.lstrip("/")
        # i.e. url_path is 'https://encode-files.s3.amazonaws.com/2017/05/12/4ae28cf4-c0a7-409f-8d8d-384ba692096a/ENCFF985JCJ.bigWig'
        if url:
            return url_path
        s3_uri = url_path.replace(url_obj.scheme, "s3")
        s3_uri = s3_uri.replace(".s3.amazonaws.com", "")
        print(s3_uri)
        return s3_uri

    def link_document(self, rec_id, document_id):
        """
        Links an existing `document` record on the Portal to some other record on the Portal via
        the latter's `documents` property.

        Args:
            rec_id: `str`. A DCC object identifier of the record to link the document to.
            document_id: `str`. An identifier of a `document` record.
        """

        # Need to compare the documents at the primary ID level (`@id` property) in order to ensure the
        # document isn't already linked. If not comparing at this identifier type and instead some
        # other type (i.e. alias, uuid), then the document will be relinked as a duplicate.

        doc_json = self.get(ignore404=False, rec_ids=document_id)
        doc_primary_id = doc_json["@id"]

        rec_json = self.get(ignore404=False, rec_ids=rec_id)
        try:
            rec_document_primary_ids = rec_json["documents"]
        except KeyError:
            # There aren't any documents at present.
            rec_document_primary_ids = []

        if doc_primary_id in rec_document_primary_ids:
            self.debug_logger.debug(
                "Will not attempt to link document {} to {} since it is already linked.".format(
                    document_id, rec_id))
            return

        # Add primary ID of new document to link.
        rec_document_primary_ids.append(doc_primary_id)
        # Originally in form of [u'/documents/ba93f5cc-a470-41a2-842f-2cb3befbeb60/',
        #                       u'/documents/tg81g5aa-a580-01a2-842f-2cb5iegcea03, ...]
        # Strip off the /documents/ prefix from each document UUID:
        payload = {}
        payload[self.ENCID_KEY] = rec_id
        payload["documents"] = rec_document_primary_ids
        self.patch(payload=payload)

# When appending "?datastore=database" to the URL. As Esther stated: "_indexer to the end of the
# URL to see the status of elastic search like
# https://www.encodeproject.org/_indexer if it's indexing it will say the status is "indexing",
# versus waiting" and the results property will indicate the last object that was indexed."
