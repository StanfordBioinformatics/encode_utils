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
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import subprocess
import urllib
import boto3

# inhouse libraries
import igvf_utils.transfer_to_gcp
import igvf_utils as iu
from igvf_utils.exceptions import (
    AwardPropertyMissing,
    FileUploadFailed,
    LabPropertyMissing,
    MissingAlias,
    ProfileNotSpecified,
    RecordIdNotPresent,
)
from igvf_utils.profiles import Profiles
import igvf_utils.utils as iuu
import igvf_utils.gc_storage

# EU-21 add support for attachment in autosql file type
mimetypes.add_type('text/autosql', '.as')

#: The directory that contains the log files created by the `Connection` class.
LOG_DIR = "IU_Logs"

BOTO3_DEFAULT_MULTIPART_CHUNKSIZE = 8_388_608
BOTO3_MULTIPART_MAX_PARTS = 10_000

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Connection:
    """Handles communication with the Portal regarding data submission and retrieval.

    For data submission or modification, and working with non-released datasets, you must have
    the environment variables `IGVF_API_KEY` and `IGVF_SECRET_KEY` set. Check with your DACC data wrangler
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

    #: Identifies the name of the key in the payload that stores a valid IGVF-assigned
    #: identifier for a record, such as alias, accession, uuid, md5sum, ... depending on
    #: the object being submitted.
    #: This is not a valid property of any IGVF object schema, and is used in the ``patch()``
    #: instance method to designate the record to update.
    IGVFID_KEY = "_igvf_id"

    #: Identifies the name of the key in the payload that stores the ID of the profile
    #: to submit to. Like ``IGVFID_KEY``, this is a non-schematic key that is used only internally.
    PROFILE_KEY = "_profile"

    #: Constant
    POST = "post"
    #: Constant
    PATCH = "patch"

    def __init__(self, igvf_mode=None, dry_run=False, submission=False, no_log_file=False):

        #: A reference to the `debug` logging instance that was created earlier in ``igvf_utils.debug_logger``.
        #: This class adds a file handler, such that all messages sent to it are logged to this
        #: file in addition to STDOUT.
        self.debug_logger = logging.getLogger(iu.DEBUG_LOGGER_NAME)

        # Be sure to set self.igvf_mode before creating the logging file handlers since the mode is
        # used as part of the file name.

        #: An indication of which Portal instance to use. Set to 'prod' for the production Portal,
        #: and 'dev' for the development Portal. Alternatively, you can set an explicit host, such as
        #: demo.igvf.org. Leaving the default of None means to use the value of the `IGVF_MODE`
        #: environment variable.
        self.igvf_modes = IgvfModes()
        for name, host in iu.IGVF_MODES.items():
            self.igvf_modes.add_mode(host["url"], mode_name=name)
        self._igvf_mode = igvf_mode
        self._profiles = None

        #: Set to True to prevent any server-side changes on the IGVF Portal, i.e. PUT, POST,
        #: PATCH, DELETE requests will not be sent to the Portal. After-POST and after-PATCH
        #: hooks (see the instance method :meth:`after_submit_hooks`) will not be run either in
        #: this case. You can turn off this dry-run feature by calling the instance method
        #: :meth:`set_live_run`.
        self.dry_run = dry_run

        #: A ``logging`` instance with a file handler for logging terse error messages.
        #: The log file resides locally within the directory specified by the constant
        #: ``connection.LOG_DIR``. Accepts messages >= ``logging.ERROR``.
        self.error_logger = logging.getLogger(iu.ERROR_LOGGER_NAME)

        #: A ``logging`` instance with a file handler for logging successful POST operations.
        #: The log file resides locally within the directory specified by the constant
        #: ``connection.LOG_DIR``. Accepts messages >= ``logging.INFO``.
        self.post_logger = logging.getLogger(iu.POST_LOGGER_NAME)
        log_level = logging.INFO
        self.post_logger.setLevel(log_level)

        # Add file handlers
        if not no_log_file:
            self._add_file_handler(logger=self.debug_logger, level=logging.DEBUG, tag="debug")
            self._add_file_handler(logger=self.error_logger, level=logging.ERROR, tag="error")
            self._add_file_handler(logger=self.post_logger, level=log_level, tag="posted")

        self.check_dry_run() #If on, signal this in the logs.

        #: Indicates whether this class is being use to submit objects to the Portal. The main
        #: effect of setting this option to True is to update the default behavior of the
        #: ``self.get()`` method, such that it it fetches its payload through the database directly
        #: rather than any index. That is useful when you are submitting several inter-dependent
        #: objects in turn and the new objects haven't yet had time to be indexed (otherwise you risk
        #: getting a 404 response back meaning "Resource Not Found". This attribute can be also set
        #: via the instance method ``self.set_submission``.
        self.set_submission(submission)  #sets self.submission attribute.
        self._auth = None

    @property
    def profiles(self):
        if self._profiles is None:
            self._profiles = Profiles(self.igvf_mode.url)
        return self._profiles

    @property
    def igvf_mode(self):
        if self._igvf_mode is None:
            igvf_mode = self._get_igvf_mode_from_env()
            self._igvf_mode = igvf_mode
        if not self.igvf_modes.has_mode(self._igvf_mode):
            self.igvf_modes.add_mode(self._igvf_mode)
        mode = self.igvf_modes.get_mode(self._igvf_mode)
        if not mode.has_been_validated:
            self._validate_igvf_mode(mode.url)
            mode.has_been_validated = True
        return mode

    def _get_igvf_mode_from_env(self):
        try:
            igvf_mode = os.environ["IGVF_MODE"]
            self.debug_logger.debug("Utilizing IGVF_MODE environment variable.")
        except KeyError:
            self.log_error((
                "ERROR: You must supply the `igvf_mode` argument or set the environment "
                "variable IGVF_MODE"
            ))
            raise
        return igvf_mode

    def _validate_igvf_mode(self, url):
        try:
            requests.get(url, timeout=2)
        except requests.exceptions.ConnectionError:
            self.log_error(
                (
                    "ERROR: The specified igvf_mode of '{}' is not valid. Should be one "
                    "of '{}' or a valid demo.igvf.org hostname."
                ).format(
                    self._igvf_mode,
                    list(iu.IGVF_MODES.keys()),
                ),
            )
            raise

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
        igvf_mode = self._igvf_mode
        if igvf_mode is None:
            igvf_mode = self._get_igvf_mode_from_env()
        bad_characters = ['/', ':']
        cleaned_igvf_mode = igvf_mode
        for c in bad_characters:
            cleaned_igvf_mode = cleaned_igvf_mode.replace(c, '')
        filename = "log_eu_" + cleaned_igvf_mode + "_" + tag + ".txt"
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

    @property
    def auth(self):
        """
        Sets the API and secret keys to use when authenticating with the DACC servers.
        These are determined from the values of the `IGVF_API_KEY` and `IGVF_SECRET_KEY`
        environment variables via the ``_get_api_keys_from_env()`` private instance
        method.
        """
        if self._auth is None:
            api_key, secret_key = self._get_api_keys_from_env()
            if api_key and secret_key:
                self._auth = (api_key, secret_key)
            else:
                self.log_error(
                    "WARNING: API keys {} not set, all functions have no permission"
                    .format(self.auth)
                )
        return self._auth

    def _get_api_keys_from_env(self):
        """
        Retrieves the API key and secret key based on the environment variables `IGVF_API_KEY` and
        `IGVF_SECRET_KEY`.

        Returns:
            `tuple`: Two item tuple containing the API Key and the Secret Key
        """
        api_key = os.environ.get("IGVF_API_KEY")
        secret_key = os.environ.get("IGVF_SECRET_KEY")
        return api_key, secret_key

    def _log_post(self, aliases, dacc_id):
        """
        Uses the self.post_logger to log a newly POSTED object's aliases and dacc_id.
        Each message is written in a three column format delimited by a tab character. The columns are:
          1) primary alias: The first alias appearing in the provided 'aliases' list argument.
          2) secondary aliases: Any additional aliases appearing in the provided 'aliases' list argument.
                 These will be comma-delimited.
          2) DACC identifier: The value of the dacc_id argument.

        Note that it is possible that the aliases list is empty, in which case only the dacc_id will
        be present in the last column of the written line. 

        Args:
            aliases: `list`. The value of the 'aliases' key in the payload for the record that
                was POSTED.
            dacc_id: `str`. An IGVF-generated identifier on the IGVF Portal for the new record
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
        entry = primary + "\t" + ",".join(secondary) + "\t" + dacc_id
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
        be called by other methods that are designed to make modifications on the IGVF Portal.

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
        self.debug_logger.error(msg)
        self.error_logger.error(msg)

    def add_alias_prefix(self, aliases, prefix=False):
        """
        Given a list of aliases, adds the lab prefix to each one that doesn't yet have a prefix set.
        The lab prefix is taken as the passed-in `prefix`, otherwise, it defaults to the `IGVF_LAB`
        environment variable, and it must be a value assigned by the DACC, i.e. "michael-snyder"
        for the Snyder Production Center. The DACC requires that aliases be prefixed in this manner. 
    
        Args:
            aliases: `list` of aliases.
            prefix: `str`. The DACC assigned lab prefix to use. If not specified, then the default
                is the value of the IGVF_LAB environment variable.
    
        Returns:
            `list`.

        Raises:
            `Exception`: A passed-in alias doesn't have a prefix set, and the default prefix could
                not be determined. 
    
        Examples::
    
              add_alias_prefix(aliases=["my-alias"],prefix="michael-snyder")
              # Returns ["michael-snyder:my-alias"]
    
              add_alias_prefix(aliases=["michael-snyder:my-alias"],prefix="michael-snyder")
              # Returns ["michael-snyder:my-alias"]

              add_alias_prefix(aliases=["my_alias"], prefix="bad-value")
              # Raises an Exception since this lab prefix isn't from a registered source record on
              # the Portal. 
    
        """
        if not prefix:
            prefix = iu.LAB_PREFIX
        prefix = prefix.strip(":")
        res = []
        for i in aliases:
            if ":" not in i:
                if not prefix:
                    raise Exception("Can't add alias prefix to aliases as it isn't specified; please set IGVF_LAB environment variable to the lab identifier assigned to you by the DACC.".format(prefix))
                else:
                    i = prefix + ":" + i
            res.append(i)
        return res

    def get_aliases(self, dacc_id, strip_alias_prefix=False):
        """
        Given an IGVF identifier for an object, performs a GET request and extracts the aliases.

        Args:
            dacc_id: `str`. The IGVF ID for a given object, i.e IGVFSM000ABC.
            strip_alias_prefix: `bool`. `True` means to remove the alias prefix if all return aliases.

        Returns:
            `list`: The aliases.
        """
        record = self.get(ignore404=False, rec_ids=dacc_id)
        aliases = record[iu.ALIAS_PROP_NAME]
        for index in range(len(aliases)):
            alias = aliases[index]
            if strip_alias_prefix:
                aliases[index] = iuu.strip_alias_prefix(alias)
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
        url = iuu.url_join([self.igvf_mode.url, "search/?"]) + query
        return url

    def search(self, search_args=[], url=None, limit=None):
        """
        Searches the Portal using the provided query parameters, which will first be URL encoded.
        The user can pass in the query parameters and values via the `search_args` argument, or
        pass in a URL directly that contains a query string via the `url` argument, or provide
        values for both arguments in which case the query parameters specified in `search_args` will
        be added to the query parameters given in the URL.

        If ``self.submission == True``, then the query will be searched with "datastore=database",
        unless the 'database' query parameter is already set. 

        Args:
            search_args: `list` of two-item tuples of the form ``[(key, val), (key, val) ,...]``.
                To support a != style query, append "!" to the key name.
            url: `str`. A URL used to search for records interactively in the IGVF Portal. The
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
        if "datastore" not in params:
            if self.submission:
                query_list.append(("datastore", "database"))

        url = self.make_search_url(search_args=query_list)
        self.debug_logger.debug("Searching DACC with query {url}.".format(url=url))
        response = requests.get(url,
                                auth=self.auth,
                                timeout=iu.TIMEOUT,
                                headers=iuu.REQUEST_HEADERS_JSON,
                                verify=False)
        status_code = response.status_code
        if not response.ok and status_code != requests.codes.NOT_FOUND:
            response.raise_for_status()
        result = response.json()["@graph"]  # the @graph object is a list
        self.debug_logger.debug("Search completed with {} hits.".format(len(result)))
        return result

    def get_profile_from_payload(self, payload):
        """
        Useful to call when doing a POST (and ``self.post()`` does call this). Ensures that the profile key
        identified by ``self.PROFILE_KEY`` exists in the passed-in payload and that the value is
        a recognized IGVF object profile (schema) identifier. Alternatively, the user can set the profile in
        the more convoluted `@id` property.

        Args:
            payload: `dict`. The intended object data to POST.

        Returns:
            `str`: The ID of the profile if all validations pass, otherwise.

        Raises:
            igvf_utils.exceptions.ProfileNotSpecified: Both keys ``self.PROFILE_KEY`` and `@id` are
              missing in the payload.
            igvf_utils.profiles.UnknownProfile: The profile ID isn't recognized by the class
                `igvf_utils.profiles.Profile`.
        """

        profile_id = payload.get(self.PROFILE_KEY)
        if not profile_id:
            profile_id = payload.get("@id")
            if not profile_id:
                raise ProfileNotSpecified(
                    ("You need to specify the ID of the profile to submit to by using the '{}' key"
                     " in the payload, or by setting the `@id` property explicitely.").format(self.PROFILE_KEY))
        profile = self.profiles.get_profile_from_id(profile_id)
        return profile

    def get_lookup_ids_from_payload(self, payload):
        """
        Given a payload to submit to the Portal, extracts the identifiers that can be used to lookup
        the record on the Portal, i.e. to see if the record already exists. Identifiers are extracted
        from the following fields:

        1. ``self.IGVFID_KEY``,
        2. aliases,
        3. md5sum (in the case of a file object)

        Args:
            payload: `dict`. The data to submit.

        Returns:
            `list`: The possible lookup identifiers.
        """
        lookup_ids = []
        if self.IGVFID_KEY in payload:
            lookup_ids.append(payload[self.IGVFID_KEY])
        if iu.ALIAS_PROP_NAME in payload:
            lookup_ids.extend(payload[iu.ALIAS_PROP_NAME])
        if "md5sum" in payload:
            # The case for file objects.
            lookup_ids.append(payload["md5sum"])

        lookup_ids = [x.strip() for x in lookup_ids]
        lookup_ids = [x for x in lookup_ids]
        if not lookup_ids:
            raise RecordIdNotPresent(
                ("The payload does not contain a recognized identifier for traceability. For example,"
                 " you need to set the 'aliases' key, or specify an IGVF assigned identifier in the"
                 " non-schematic key {}.".format(self.IGVFID_KEY)))

        return lookup_ids


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
            frame: `str`. A value for the frame query parameter, i.e. 'object', 'edit'.
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
            url = iuu.url_join([self.igvf_mode.url, r, "?format=json"])
            if database:
                url += "&datastore=database"
            if frame:
                url += "&frame={frame}".format(frame=frame)
            self.debug_logger.debug(">>>>>>GET {rec_id} From DACC with URL {url}".format(
                rec_id=r, url=url))
            response = requests.get(url,
                                    auth=self.auth,
                                    timeout=iu.TIMEOUT,
                                    headers=iuu.REQUEST_HEADERS_JSON,
                                    verify=False)
            if response.ok:
                return response.json()
            status_codes[response.status_code] = r

        if requests.codes.FORBIDDEN in status_codes:
            raise Exception(
                "Access to IGVF record {} is forbidden".format(status_codes[requests.codes.FORBIDDEN]))
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

        Checks if the provided file is an image in either of the JPEG 
        or TIFF formats - if so, then checks the image orientation in the EXIF data and rotates if
        if necessary. The original image will not be modified.

        Args:
            document: `str`. A local file path.

        Returns:
            `dict`: The 'attachment' propery value.
        """
        download_filename = os.path.basename(document)
        (mime_type, extension) = mimetypes.guess_type(download_filename)
        # Because the portal treats all gzipped files as application/gzip,
        # force the type to be application/gzip for any files with .gz
        # extension, even if the content is another type.
        if extension == 'gzip':
            mime_type = 'application/gzip'
        data = None
        if iuu.is_jpg_or_tiff(document):
            orientation_stats = iuu.orient_jpg(document)
            if orientation_stats["transformed"]:
                self.debug_logger.debug("Image {} orientation transformed from {} to {}.".format(download_filename, orientation_stats["from"], 1))
                data = base64.b64encode(orientation_stats["stream"])
        if not data:
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
        if profile_id not in self.profiles.FILE_PROFILE_ID:
            return
        self.upload_file(file_id=rec_id)

    def after_submit_hooks(self, rec_id, profile_id, method="", upload_file=True):
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
            upload_file: `bool`. If `False`, skip uploading files to the Portal.
                Defaults to `True`.
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
        if upload_file is False:
            self.debug_logger.debug(
                "Will not upload file %s to the portal since upload_file is False",
                rec_id,
            )
        if method == self.POST and upload_file is True:
            self.after_submit_file_cloud_upload(rec_id, profile_id)

        # Call PATCH-specific hooks if PATCH:
        #... None yet.

    def before_submit_alias(self, payload):
        """
        A pre-POST and pre-PATCH hook used to 
          1) Clean alias names by removing disallowed characters indicated by the DACC schema for
             the alias property. 
          2) Add the lab alias prefix to any aliases that are missing it. 
             The `IGVF_LAB` environment variable is consulted to fetch the lab name, and if not
             set then this will be a no-op.

        Args:
            payload: `dict`. The payload to submit to the Portal.

        Returns:
            `dict`: The potentially modified payload.
        """
        if not iu.ALIAS_PROP_NAME in payload:
            return payload
        aliases = iuu.clean_aliases(payload[iu.ALIAS_PROP_NAME])
        aliases = self.add_alias_prefix(aliases)
        payload[iu.ALIAS_PROP_NAME] = sorted(set(aliases))
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
        attachment_props = [
            "attachment",
        ]
        path = "path"

        for prop in attachment_props:
            if prop in payload:
                val = payload[prop]  # dict
                if path in val:
                    # Then set the actual attachment object:
                    attachment = self.set_attachment(document=val[path])
                    payload[prop] = attachment
        return payload

    def before_post_file(self, payload):
        """
        A pre-POST hook that calculates and sets the `md5sum` property and `file_size` property
        for a file record. However, any of these two properties that is already set in the payload 
        to a non-empty value will not be reset. 

        Args:
            payload: `dict`. The payload to submit to the Portal.

        Returns:
            `dict`: The potentially modified payload.

        Raises:
            igvf_utils.utils.MD5SumError: Perculated through the function
              `igvf_utils.utils.calculate_md5sum` when it can't calculate the md5sum.
        """
        profile_id = payload[self.PROFILE_KEY]
        if profile_id not in self.profiles.FILE_PROFILE_ID:
            return payload
        try:
            file_name = payload[self.profiles.SUBMITTED_FILE_PROP_NAME]
        except KeyError:
            return payload
        # Set md5sum
        if (self.profiles.MD5SUM_NAME_PROP_NAME in payload) and (payload[self.profiles.MD5SUM_NAME_PROP_NAME]):
            # Already set; nothing to do.
            pass
        else:
            payload[self.profiles.MD5SUM_NAME_PROP_NAME] = iuu.calculate_md5sum(file_name)
        # Set file_size
        if (self.profiles.FILE_SIZE_PROP_NAME in payload) and (payload[self.profiles.FILE_SIZE_PROP_NAME]):
            # Already set; nothing to do.
            pass
        else:
            payload[self.profiles.FILE_SIZE_PROP_NAME] = iuu.calculate_file_size(file_name)
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
        if profile_id not in self.profiles.FILE_PROFILE_ID:
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

    def post(
        self,
        payload,
        require_aliases=True,
        upload_file=True,
        return_original_status_code=False,
        truncate_long_strings_in_payload_log=False,
    ):
        """POST a record to the Portal.

        Requires that you include in the payload the non-schematic key ``self.PROFILE_KEY`` to
        designate the name of the IGVF object profile that you are submitting to, or the
        actual `@id` property itself.

        If the `lab` property isn't present in the payload, then the default will be set to the value
        of the `IGVF_LAB` environment variable. Similarly, if the `award` property isn't present, then the
        default will be set to the value of the `IGVF_AWARD` environment variable.

        Before the POST is attempted, any pre-POST hooks are fist called; see the method
        ``self.before_submit_hooks``).  After a successfuly POST, any after-POST submit hooks are
        also run; see the method ``self.after_submit_hooks``. 

        Args:
            payload: `dict`. The data to submit.
            require_aliases: `bool`. `True` means that the 'aliases' property is to be required in
                 `payload`. This is the default and it is highly recommended not to change this
                 because it'll be easy to create duplicates on the server if accidentally POSTING
                 the same payload again. For example, you can easily create the same biosample
                 as many times as you want on the Portal when not providing an alias. Furthermore,
                 submitting labs should include at least one alias per record being submitted
                 to the Portal for traceabilty purposes in the submitting lab.
            upload_file: `bool`. If `False`, when POSTing files the file data will not
                be uploaded to S3, defaults to `True`. This can be useful if you have
                custom upload logic. If the files to upload are already on disk, it is
                recommmended to leave this with the default, which will use `aws s3 cp`
                to upload them.
            return_original_status_code: `bool`. Defaults to `False`. If `True`, then
                will return the original `requests.Response.status_code` of the initial
                post, in addition to the usual `dict` response.
            truncate_long_strings_in_payload_log: `bool`. Defaults to `False`. If
                `True`, then long strings (> 1000 characters) present in the payload
                will be truncated before being logged.

        Returns:
            `dict`: The JSON response from the POST operation, or the existing record if it already
            exists on the Portal (where a GET on any of it's aliases, when provided in the payload,
            finds the existing record). If `return_original_status_code=True`, then will
            return a `tuple` of the above `dict` and an `int` corresponding to the
            status code on POST of the initial payload.

        Raises:
            igvf_utils.exceptions.AwardPropertyMissing: The `award` property isn't present in the payload and there isn't a
                default set by the environment variable `IGVF_AWARD`.
            igvf_utils.exceptions.LabPropertyMissing: The `lab` property isn't present in the payload and there isn't a
                default set by the environment variable `IGVF_LAB`.
            igvf_utils.exceptions.MissingAlias: The argument 'require_aliases' is set to True and
                the 'aliases' property is missing in the payload or is empty.
            requests.exceptions.HTTPError: The return status is not ok.

        Side effects:
            self.PROFILE_KEY will be popped out of the payload if present, otherwise, the key "@id"
            will be popped out. Furthermore, self.IGVFID_KEY will be popped out if present in the payload.
        """
        self.debug_logger.debug("\nIN post().")
        # Make sure we have a payload that can be converted to valid JSON, and
        # tuples become arrays, ...
        payload = json.loads(json.dumps(payload))
        profile = self.get_profile_from_payload(payload)
        payload[self.PROFILE_KEY] = profile.name
        url = iuu.url_join([self.igvf_mode.url, profile.name])
        if self.IGVFID_KEY in payload:
            # Shouldn't be here, unless maybe a PATCH was attempted and the record didn't exist, so
            # a POST was then attempted.
            payload.pop(self.IGVFID_KEY)
        # Check if we need to add defaults for 'award' and 'lab' properties:
        if profile.has_award:  # No lab prop for these profiles either.
            if iu.AWARD_PROP_NAME not in payload:
                if not iu.AWARD:
                    raise AwardPropertyMissing
                payload.update(iu.AWARD)
            if iu.LAB_PROP_NAME not in payload:
                if not iu.LAB:
                    raise LabPropertyMissing
                payload.update(iu.LAB)

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
        aliases = payload.get(iu.ALIAS_PROP_NAME)
        if not aliases:
            if not profile.has_alias or not require_aliases:
                aliases = ["N/A"]
                no_alias = True
            else:
                raise MissingAlias(
                    ("Missing property '{}' in payload {}. This is required by default for the profiles"
                     " that include this property, and can be disabled by setting the `require_aliases`"
                     " argument to False in the call to this method, being `igvf_utils.connection.Connection.post()`").format(iu.ALIAS_PROP_NAME,payload))

        # Validate the payload against the schema
        ### This doesn't work as locally I can't use jsonschema to validate a profile with
        ### custom objects specified in the value of a linkTo property.
        self.debug_logger.debug("Validating the payload against the schema")
        validation_error = iuu.err_context(payload=payload, schema=self.profiles.get_profile_from_id(profile.name).schema)
        if validation_error:
           self.log_error("Invalid schema instance of the {} profile.".format(profile.name))
           self.debug_logger.debug("Payload is: {}".format(iuu.print_format_dict(payload)))
           self.log_error(validation_error[0]) # The top-level validation message
           if validation_error[1]: # The validation context can be empty
               self.debug_logger.debug(iuu.print_format_dict(validation_error[1]))
           raise Exception(iuu.print_format_dict(validation_error[0]))

        self.debug_logger.debug(
            (
                "<<<<<< POST {} record {alias} To IGVF database with URL {url} and this payload:"
                "\n\n{payload}\n\n"
            ).format(
                profile.name,
                alias=aliases[0],
                url=url,
                payload=iuu.print_format_dict(
                    payload,
                    truncate_long_strings=truncate_long_strings_in_payload_log
                )
            )
        )

        if self.check_dry_run():
            return {}
        response = requests.post(url,
                                 auth=self.auth,
                                 timeout=iu.TIMEOUT,
                                 headers=iuu.REQUEST_HEADERS_JSON,
                                 json=payload,
                                 verify=False)
        #response_json = response.json()["@graph"][0]
        response_json = response.json()
        original_status_code = response.status_code

        if response.ok:
            self.debug_logger.debug("Success.")
            response_json = response_json["@graph"][0]
            encid = ""
            try:
                encid = response_json["accession"]
            except KeyError:
                # Some objects don't have an accession, i.e. replicates.
                encid = response_json["uuid"]
            self.debug_logger.debug(f"Object posted with identifier: {encid}")
            self._log_post(aliases=aliases, dacc_id=encid)
            # Run 'after' hooks:
            self.after_submit_hooks(encid, profile.name, method=self.POST, upload_file=upload_file)
            if return_original_status_code is True:
                return (response_json, original_status_code)
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
                    if return_original_status_code is True:
                        return (existing_record, original_status_code)
                    return existing_record

        else:
            message = "Failed to POST {}".format(aliases[0])
            self.log_error(message)
            self.debug_logger.debug("<<<<<< DACC POST RESPONSE: ")
            self.debug_logger.debug(iuu.print_format_dict(response_json))
            response.raise_for_status()

    def patch(self, payload, raise_403=True, extend_array_values=True):
        """PATCH a record on the Portal.

        Before the PATCH is attempted, any pre-PATCH hooks are fist called (see the method
        ``self.before_submit_hooks()``). If the PATCH fails due to the resource not being found (404),
        then that fact is logged to both the debug and error loggers.

        Args:
            payload: `dict`. containing the attribute key and value pairs to patch. Must contain the key
                ``self.IGVFID_KEY`` in order to indicate which record to PATCH.
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
            KeyError: The payload doesn't have the key ``self.IGVFID_KEY`` set AND there aren't
                any aliases provided in the payload's 'aliases' key.
            requests.exceptions.HTTPError: if the return status is not ok (excluding a
                403 status if 'raise_403' is False.
        """
        # Make sure we have a payload that can be converted to valid JSON, and
        # tuples become arrays, ...
        payload = json.loads(json.dumps(payload))
        self.debug_logger.debug("\nIN patch()")
        igvf_id = payload[self.IGVFID_KEY]
        # Ensure that the record exists on the Portal:
        rec_json = self.get(rec_ids=igvf_id, frame="edit", ignore404=True)
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

                    ## CHECK FOR DUPLICATES: Be careful as some can be tricky, i.e.
                    # ['/documents/id1', 'id1']
                    # such a duplicate should be identified and removed, leaving us with ["id1"].
                    # Checks for arrays of strings or of dicts.
                    if len(val) == 0:
                        continue
                    if isinstance(val[0], str):
                        profile_id = self.get_profile_from_payload(payload)
                        payload[key] = self.profiles.remove_duplicate_associations(val)
                    elif isinstance(val[0], dict):
                        payload[key] = iuu.remove_duplicate_objects(val)

        # Run 'before' hooks:
        payload = self.before_submit_hooks(payload, method=self.PATCH)
        payload.pop(self.IGVFID_KEY)
        if self.PROFILE_KEY in payload:
            # Some client software may add this key in; won't hurt to remove it.
            payload.pop(self.PROFILE_KEY)

        url = iuu.url_join([self.igvf_mode.url, igvf_id.lstrip("/")])
        self.debug_logger.debug(
            ("<<<<<< PATCHING {igvf_id} To IGVF database with URL"
             " {url} and this payload:\n\n{payload}\n\n").format(
                 igvf_id=igvf_id, url=url, payload=iuu.print_format_dict(payload)))

        if self.check_dry_run():
            return {}
        response = requests.patch(url, auth=self.auth, timeout=iu.TIMEOUT, headers=iuu.REQUEST_HEADERS_JSON,
                                  json=payload, verify=False)
        response_json = response.json()

        if response.ok:
            self.debug_logger.debug("Success.")
            response_json = response_json["@graph"][0]
            uuid = response_json["uuid"]
            profile_id = self.profiles.get_profile_from_id(response_json["@id"]).name
            # Run 'after' hooks:
            self.after_submit_hooks(uuid, profile_id, method=self.PATCH)
            return response_json
        elif response.status_code == requests.codes.FORBIDDEN:
            # Don't have permission to PATCH this object.
            if not raise_403:
                return rec_json

        message = "Failed to PATCH {}".format(igvf_id)
        self.log_error(message)
        self.debug_logger.debug("<<<<<< PATCH RESPONSE: ")
        self.debug_logger.debug(iuu.print_format_dict(response_json))
        response.raise_for_status()

    def remove_props(self, rec_id, props=[]):
        """Runs a PUT request to remove properties of interest on the specified record.

        Note that before-submit and after-submit hooks are not run here as they would be in
        `self.patch()` or `self.post()` (:meth:`before_submit_hooks` and :meth:`after_submit_hooks`
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
        profile = self.profiles.get_profile_from_id(rec_json["@id"])
        del rec_json
        editable_json = self.get(rec_ids=rec_id, frame="edit", ignore404=False)
        # For good house-keeping, check for any props that we definitely aren't allowed to remove,
        # and raise an Exception if one is present in the supplied 'props' list. Some properties,
        # such as accession, submitted_by, ..., still show up in a GET with 'frame="edit"', and
        # the Portal will most likely complain or silently disallow an attempt to remove such
        # properites. Nonetheless, a well-behaved client shouldn't send uncouth requests, so some
        # checking is performed below for good measure:
        for prop_name in props:
            prop = profile.get_property_from_name(prop_name)
            if prop.is_required:
                raise Exception("Can't remove required property")
            elif prop.is_not_submittable:
                raise Exception("Can't remove non-submittable property.")
            elif prop.is_read_only:
                raise Exception("Can't remove read-only property.")
            else:
                # Then it is safe to remove this property.
                editable_json.pop(prop.name)

        url = iuu.url_join([self.igvf_mode.url, rec_id])
        self.debug_logger.debug(
            "Attempting to remove properties {} from record '{}' by "
            "sending a PUT request with payload {}.".format(
                props, rec_id, iuu.print_format_dict(editable_json))
            )
        if self.check_dry_run():
            return
        response = requests.put(
            url,
            auth=self.auth,
            timeout=iu.TIMEOUT,
            headers=iuu.REQUEST_HEADERS_JSON,
            json=editable_json,
            verify=False
        )
        response.raise_for_status()
        self.debug_logger.debug("Success")
        response_json = response.json()
        return response_json

    def remove_and_patch(
        self,
        props,
        patch,
        raise_403=True,
        extend_array_values=True
    ):
        """Runs a PUT request to remove properties and patch a record in one
        request.

        In general, this is a method combining ``remove_props`` and ``patch``.
        This is useful because some schema dependencies requires property
        removal and property patch (including adding new properties) happening
        at the same time. Please note that after the record is retrieved from
        the portal, ``props`` will be removed before the ``patch`` is applied.

        Args:
            props: `list`. The properties to remove from the record.
            patch: `dict`. containing the attribute key and value pairs to
                patch. Must contain the key ``self.IGVFID_KEY`` in order to
                indicate which record to PATCH.
            raise_403: `bool`. `True` means to raise a
                ``requests.exceptions.HTTPError`` if a 403 status (forbidden)
                is returned. If set to `False` and there still is a 403 return
                status, then the object you were trying to PATCH will be
                fetched from the Portal in JSON format as this function's
                return value.
            extend_array_values: `bool`. Only affects keys with array values.
                `True` (default) means to extend the corresponding value on the
                Portal with what's specified in the payload. `False` means to
                replace the value on the Portal with what's in the payload.

        Returns:
            `dict`: The JSON response from the PUT operation, or an empty dict
                if the record doesn't exist on the Portal. Will also be an
                empty dict if the dry-run feature is enabled.

        Raises:
            requests.exceptions.HTTPError: if the return status is not ok
                (excluding a 403 status if 'raise_403' is False).
        """
        if not props:
            raise ValueError(
                'Input props to remove must be non-empty. If you only need to '
                'patch and not remove properties, use the patch() method of '
                'the Connection class.'
            )
        if set(patch.keys()) <= {self.IGVFID_KEY, self.PROFILE_KEY}:
            raise ValueError(
                'Input patch has no valide properties to patch. If you only '
                'need to patch and not remove properties, use the '
                'remove_props() method of the Connection class.'
            )
        # Make sure we have a payload that can be converted to valid JSON, and
        # tuples become arrays, ...
        patch = json.loads(json.dumps(patch))
        self.debug_logger.debug("\nIN remove_and_patch()")
        igvf_id = patch[self.IGVFID_KEY]
        rec_json = self.get(rec_ids=igvf_id, frame="object", ignore404=True)
        if not rec_json:  # Ensure that the record exists on the Portal:
            return {}
        profile = self.profiles.get_profile_from_id(rec_json["@id"])
        payload = self.get(rec_ids=igvf_id, frame="edit", ignore404=False)
        for prop_name in props:
            prop = profile.get_property_from_name(prop_name)
            if prop.is_required:
                raise Exception("Can't remove required property")
            elif prop.is_not_submittable:
                raise Exception("Can't remove non-submittable property.")
            elif prop.is_read_only:
                raise Exception("Can't remove read-only property.")
            else:
                # Then it is safe to remove this property.
                payload.pop(prop.name, None)
        for key in patch:
            if all([
                extend_array_values,
                key in payload,
                isinstance(patch[key], list),
            ]):
                val = payload[key]
                val.extend(patch[key])
                # CHECK FOR DUPLICATES: Be careful as some can be tricky, i.e.
                # ['/documents/id1', 'id1'] such a duplicate should be
                # identified and removed, leaving us with ["id1"].
                # Checks for arrays of strings or of dicts.
                if len(val) == 0:
                    continue
                if isinstance(val[0], str):
                    payload[key] = self.profiles.remove_duplicate_associations(val)
                elif isinstance(val[0], dict):
                    payload[key] = iuu.remove_duplicate_objects(val)
            else:
                payload[key] = patch[key]

        # Run 'before' hooks:
        payload = self.before_submit_hooks(payload, method=self.PATCH)
        payload.pop(self.IGVFID_KEY)
        payload.pop(self.PROFILE_KEY, None)

        url = iuu.url_join([self.igvf_mode.url, igvf_id.lstrip("/")])
        self.debug_logger.debug(
            "<<<<<< PUTTING {igvf_id} To DACC with URL"
            " {url} and this payload:\n\n{payload}\n\n".format(
                igvf_id=igvf_id,
                url=url,
                payload=iuu.print_format_dict(payload)
            )
        )

        if self.check_dry_run():
            return {}
        response = requests.put(
            url,
            auth=self.auth,
            timeout=iu.TIMEOUT,
            headers=iuu.REQUEST_HEADERS_JSON,
            json=payload,
            verify=False
        )
        response_json = response.json()

        if response.ok:
            self.debug_logger.debug("Success.")
            response_json = response_json["@graph"][0]
            uuid = response_json["uuid"]
            profile_id = self.profiles.get_profile_from_id(response_json["@id"]).name
            # Run 'after' hooks:
            self.after_submit_hooks(uuid, profile_id, method=self.PATCH)
            return response_json
        elif response.status_code == requests.codes.FORBIDDEN:
            # Don't have permission to PATCH this object.
            if not raise_403:
                return rec_json

        message = "Failed to PUT {}".format(igvf_id)
        self.log_error(message)
        self.debug_logger.debug("<<<<<< DACC PUT RESPONSE: ")
        self.debug_logger.debug(iuu.print_format_dict(response_json))
        response.raise_for_status()


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
        Given an IGVF experiment ID, gets its JSON representation from the Portal and looks in the
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

        response = requests.post(
            iuu.url_join([self.igvf_mode.url, "files", file_id, "@@upload"]),
            auth=self.auth,
            headers=iuu.REQUEST_HEADERS_JSON,
            json = {},
            timeout=iu.TIMEOUT)
        response_json = response.json()
        if response.ok:
            self.debug_logger.debug("Success: upload credentials for '{}' regenerated.".format(file_id))
            upload_creds = response_json["@graph"][0]["upload_credentials"]
            return upload_creds
        else:
            status_code = response.status_code
            err_msg = "Error {}: unable to re-issue upload credentials for '{}'".format(status_code, file_id)
            self.log_error(err_msg)
            self.debug_logger.debug(iuu.print_format_dict(response_json))
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

        # Don't log the full response as it contains sensitive security information.

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
            # One with IGVF API keys can get the URL in a more straightforward manner by doing a GET on
            # the files @@upload endpoint. But this even requires AWS keys even when the file in 
            # question is released. For broader community support, the above workaround is in use.
            rec = self.get(i, ignore404=False)
            md5 = base64.b64encode(bytes.fromhex(rec["md5sum"]))
            fout.write("\t".join([url, str(rec["file_size"]), md5.decode("utf-8")]) + "\n")
        fout.close()

    def gcp_transfer_from_aws(self, file_ids, gcp_bucket, gcp_project, description="", aws_creds=()):
        """
        Copies one or more IGVF files from AWS S3 storage to GCP storage by using the Google STS.
        This is similar to the :meth:`gcp_transfer_urllist` method - the difference is that S3 object
        paths are copied directly instead of using public HTTPS URIs, and AWS keys are required here. 

        See :func:`igvf_utils.transfer_to_gcp.Transfer` for full documentation.

        Args:
            file_ids: `list`. One or more IGVF files to transfer. They can be any valid IGVF File
                object identifier. Don't mix IGVF files from across buckets.
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
        t = igvf_utils.transfer_to_gcp.Transfer(gcp_project=gcp_project, aws_creds=aws_creds)
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
        Uploads a file to the Portal for the indicated file record. The file to upload can be
        specified by setting the `file_path` parameter, or by using the value of the IGVF file 
        profile's `submitted_file_name` property of the given file object represented by the 
        `file_id` parameter. The file to upload can be from any of the following sources:

          1. Path to a local file,
          2. S3 object, or
          3. Google Storage object

        For the AWS option above, the user must set the proper AWS keys, see the `wiki documentation`_.

        If the dry-run feature is enabled, then this method will return prior to launching the
        upload command.

        Args:
            file_id: `str`. An identifier of a `file` record on the IGVF Portal.
            file_path: `str`. The local path to the file to upload, or an S3 object (i.e s3://mybucket/test.txt),
              or a Google Storage object (i.e. gs://mybucket/test.txt).
              If not set, defaults to `None` in which case the local file path will be extracted from the
              record's `submitted_file_name` property.
            set_md5sum: `bool`. True means to also calculate the md5sum and set the file record's `md5sum`
              property on the Portal (this currently is only implemented for local files and S3; not yet GCP).
              This will always take place whenever the property isn't yet set.
              Furthermore, setting to True will also cause the `file_size` property to be set. 
              Normally these two properties would already be set as they are required in the *file* profile,
              however, if the wrong file was originally uploaded, then they must be reset when 
              uploading a new file. 

        Raises:
            igvf_utils.exceptions.FileUploadFailed: The return code of the AWS upload command was non-zero.

        .. _`wiki documentation`: https://github.com/IGVF-DACC/igvf_utils/wiki/Configuration#aws-keys
        """
        self.debug_logger.debug("\nIN upload_file()\n")
        #upload_credentials = self.get_upload_credentials(file_id) # Don't use this - they may have expired.
        upload_credentials = self.regenerate_aws_upload_creds(file_id)
        aws_creds = self.extract_aws_upload_credentials(upload_credentials)
        file_rec = self.get(rec_ids=file_id,ignore404=False)
        if not file_path:
            try:
                file_path = file_rec[self.profiles.SUBMITTED_FILE_PROP_NAME]
            except KeyError:  # submitted_file_name property not set:
                raise Exception("No file path specified.")
        file_rec_md5sum = file_rec.get(self.profiles.MD5SUM_NAME_PROP_NAME)
        if not file_rec_md5sum or set_md5sum:
            # md5sum calc. supported at present only for local files and aws (not GCP)
            self.debug_logger.debug("Calculating md5sum for {}".format(os.path.basename(file_path)))
            md5sum = iuu.calculate_md5sum(file_path)
            file_size = iuu.calculate_file_size(file_path)
            self.patch({self.IGVFID_KEY: file_rec["@id"],
                        self.profiles.MD5SUM_NAME_PROP_NAME: md5sum,
                        self.profiles.FILE_SIZE_PROP_NAME: file_size})

        cmd_args = "{file_path} {upload_url}".format(file_path=file_path, upload_url=aws_creds["UPLOAD_URL"])
        if file_path.startswith("gs://"):
            gs_file = igvf_utils.gc_storage.GSFile(name=file_path)
            s3 = boto3.client(
                "s3",
                aws_access_key_id=aws_creds["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=aws_creds["AWS_SECRET_ACCESS_KEY"],
                aws_session_token=aws_creds["AWS_SESSION_TOKEN"],
            )
            s3_uri = aws_creds["UPLOAD_URL"]
            path_parts = s3_uri.replace("s3://", "").split("/")
            bucket = path_parts.pop(0)
            key = "/".join(path_parts)
            multipart_chunksize = self._calculate_multipart_chunksize(gs_file.size)
            transfer_config = boto3.s3.transfer.TransferConfig(
                multipart_chunksize=multipart_chunksize
            )
            self.debug_logger.debug("Uploading file %s to %s", gs_file.filename, s3_uri)
            s3.upload_fileobj(gs_file, bucket, key, Config=transfer_config)
            self.debug_logger.debug("Finished uploading file %s", gs_file.filename)
            return
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

    def _calculate_multipart_chunksize(self, file_size_bytes: int) -> int:
        """
        Calculates the `multipart_chunksize` to use for `boto3` `TransferConfig` to
        ensure that the file can be uploaded successfully without reaching the 100000
        part limit. The default values are the same as the defaults for `TransferConfig`
        """
        multipart_chunksize = BOTO3_DEFAULT_MULTIPART_CHUNKSIZE * (
            max((file_size_bytes - 1), 0)
            // (BOTO3_MULTIPART_MAX_PARTS * BOTO3_DEFAULT_MULTIPART_CHUNKSIZE)
            + 1
        )
        return multipart_chunksize

    def get_platforms_on_experiment(self, rec_id):
        """
        Looks at all FASTQ files on the specified experiment, and tallies up the varying sequencing
        platforms that generated them.  The platform of a given file record is indicated by the
        `platform` property. This is moreless used to verify that there aren't a mix of
        multiple different platforms present as normally all reads should come from the same platform.

        Args:
            rec_id: `str`. DACC identifier for an experiment.
        Returns:
            `list`: The de-duplicated list of platforms seen on the experiment's FASTQ files.
        """
        fastq_files = self.get_fastqfiles_on_exp(rec_id)
        platforms = []
        for fastq_json in fastq_files:
            platforms.extend(fastq_json["platform"][iu.ALIAS_PROP_NAME])
        return list(set(platforms))

    def post_document(self, document, document_type, description):
        """POSTS a document to the Portal.

        The alias for the document will be the lab prefix plus the file name. The lab prefix is taken
        as the value of the `IGVF_LAB` environment variable, i.e. 'michael-snyder'.

        Args:
            document_type: `str`. For possible values, see
              https://igvf-ui-dev.demo.igvf.org/profiles/document. It appears that one should use
              "data QA" for analysis results documents.
            description: `str`. The description for the document.
            document: `str`. Local file path to the document to be submitted.

        Returns:
            `str`: The DACC UUID of the new document.
        """
        document_filename = os.path.basename(document)
        document_alias = iu.LAB[iu.LAB_PROP_NAME] + ":" + document_filename
        mime_type = mimetypes.guess_type(document_filename)[0]
        if not mime_type:
            raise Exception("Couldn't guess MIME type for {}.".format(document_filename))

        # Post information
        payload = {}
        payload[self.PROFILE_KEY] = "document"
        payload[iu.ALIAS_PROP_NAME] = [document_alias]
        payload["document_type"] = document_type
        payload["description"] = description

        attachment = self.set_attachment(document)

        payload['attachment'] = attachment

        response = self.post(payload=payload)
        return response['uuid']

    def download(self, rec_id, get_stream=False, directory=None):
        """
        Downloads the contents of the specified file or document object from the IGVF Portal to
        either the calling directory or the indicated download directory. The downloaded file will
        be named as it is on the Portal.

        Alternatively, you can get a reference to the response object by setting the `get_stream`
        parameter to True. Useful if you want to inspect the response, i.e. see if there was a 
        redirect and where to, or download the byte stream in a customized manner.

        Args:

           rec_id: `str`. An identifier for a file or document record on the Portal.
           directory: `str`. The full path to the directory in which to download the file. If not
               specified, then the file will be downloaded in the calling directory.

        Returns:
            `str`. The full path to the downloaded file if the `get_stream` parameter is False.
            `requests.models.Response`: The `get_stream` parameter is True.
        """
        rec = self.get(rec_id, ignore404=False)
        # Check whether we need to download a Document or File record.
        rec_type = rec["@type"]
        if "Document" in rec_type:
            file_type = False
            auth = ()
        elif "File" in rec_type:
            file_type = True
            auth = self.auth
        else:
            raise Exception("This method can only download records of type 'File' and 'Document'; '{}' is neither of these.".format(rec_id))
        # Formulate download URL:
        if file_type:
            url = iuu.url_join([self.igvf_mode.url, rec["href"].lstrip("/")])
        else:
            url = iuu.url_join([self.igvf_mode.url, "documents", rec["uuid"], rec["attachment"]["href"]])
        r = requests.get(
            url,
            auth=auth,
            stream = True,
            timeout=iu.TIMEOUT,
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
        Given an IGVF File object's id (such as accession, uuid, alias), returns the full S3 object
        URI, or HTTP/HTTPS URI if url=True. 

        Args:
            rec_id: `str`. An IGVF object identifier of the record to link the document to.
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


class IgvfMode:
    DEFAULT_SCHEME = "https"

    def __init__(self, host_or_url):
        self._host_or_url = host_or_url
        self.has_been_validated = False

    @property
    def host(self):
        """
        Returns:
            `str`. The URL without scheme.
        """
        parsed = urllib.parse.urlparse(self._host_or_url)
        url_without_scheme = urllib.parse.ParseResult("", *parsed[1:]).geturl()
        return url_without_scheme.lstrip("/")

    @property
    def url(self):
        """
        Returns:
            `str`. The URL with scheme.
        """
        parsed = urllib.parse.urlparse(self._host_or_url, scheme=self.DEFAULT_SCHEME)
        if parsed.netloc:
            return urllib.parse.urlunparse(parsed)
        url = urllib.parse.ParseResult(
            parsed.scheme,
            parsed.path,
            "",
            *parsed[3:],
        ).geturl()
        return url


class IgvfModes:
    def __init__(self):
        self._modes = {}

    def add_mode(self, host_or_url, mode_name=None):
        new_mode = IgvfMode(host_or_url)
        name = mode_name if mode_name is not None else host_or_url
        self._modes[name] = new_mode

    def get_mode(self, mode_name):
        return self._modes[mode_name]

    def has_mode(self, mode_name):
        return mode_name in self._modes
