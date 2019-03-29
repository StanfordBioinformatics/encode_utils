# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Contains utilities that don't require authorization on the DCC servers.
"""

import hashlib
import io
import json
import jsonschema
import logging
import os
import PIL.Image
import requests
import subprocess

import exifread

import encode_utils as eu
import encode_utils.aws_storage


#: Stores the HTTP headers to indicate JSON content in a request.
REQUEST_HEADERS_JSON = {'content-type': 'application/json'}

def is_jpg_or_tiff(filename):
    """
    Checks if the provided file is an image file that is formatted as either JPEG or TIFF.

    Args: 
        filename: `str`. Local file. 

    Returns:
        `False`: The provided file is not a JPEG or TIFF image.
        `str`: 'JPEG' if this is a JPEG image, or 'TIFF' if this is a TIFF image. 

    Raises:
        `OSError`: The provided file isn't a recognized image format.
    """
    TIFF = "TIFF"
    JPEG = "JPEG"
    try:
      img = PIL.Image.open(filename)
    except OSError:
      # Raised when input file isn't a recognized image format.
      return False
    if img.format == JPEG:
        return JPEG
    elif img.format == TIFF:
        return TIFF
    else:
        return False

def orient_jpg(image):
    """
    Given a JPG or TIFF, attempts to read the EXIF data to determine the orientation and then
    transform the image if needed. This function is called in `connection.Connection.set_attachment()`.

    EXIF - exchangeable image file format - is only supported by JPG and TIFF formatted images. 
    Such images aren't even required to set EXIF metadata. Imaging software sometimes sets EXIF 
    to allow clients to read metadata such as what software took the picture, and what orientation
    it's in. This function is concerned with the oriention being in an upright position.

    Note! Existing EXIF data will be lost for any transformed image. That's not a big issue for 
    orientation, however, since software should consider the orientation to be 1 when EXIF isn't 
    present anyways.

    Args:
        image: `str` or `bytes` instance.  Use a string to supply the path to local JPG or TIFF file.
            Use a bytes object if you have the image data already in memory. 

    Returns:
        `dict`: Dictionary with keys being:

            #. from - int. The orientation that was read in, or 0 if unknown.
            #. transformed - boolean. True if this function transformed the image, False otherwise. 
               Note that False could either mean that the image didn't need any transformation or that
               the need for a transformation could not be determined based on EXIF metadata or lack thereof. 
            #. stream - A `bytes` instance.

    Raises:
        `InvalidExifOrientation`: The EXIF orientation data is present, but the orientation value
            isn't in the expected range of [1..8]. 
    """
    class InvalidExifOrientation(Exception):
        """
        Raised when the EXIF orientation is set, but the value is not interpretable.
        """
        pass

    UNKNOWN_ORIENTATION_VALUE = 0
    orientation = UNKNOWN_ORIENTATION_VALUE
    img = PIL.Image.open(image)
    try:
        if img.format == "JPEG":
            exif = img._getexif()
            if exif:
                orientation = exif[274] # int in [1..8]
        elif img.format == "TIFF":
            tags = exifread.process_file(open(image, "rb")) # tags is dict.
            if tags:
                ORIENT_KEY = "Image Orientation"
                if ORIENT_KEY in tags:
                    orient_val = tags[ORIENT_KEY]
                    if orient_val:
                        orientation = orient_val.values[0]
    except (AttributeError, KeyError):
        # Maybe this image doesn't use EXIF data, or it does but the orientation field is absent.
        pass
    degrees = None
    flip = None
    if orientation in [0, 1]:
        pass
    elif orientation == 8:
        degrees = 90
    elif orientation == 3:
        degrees = 180
    elif  orientation == 6:
        degrees = 270
    elif orientation == 2:
        flip = True
    elif orientation == 7:
        degrees = 90
        flip = True
    elif orientation == 4:
        degrees = 180
        flip = True
    elif orientation == 5:
        degrees = 270 
        flip = True
    else:
        raise InvalidExifOrientation("Unknown exif orientation value {}.".format(orientation))
    
    transformed = True
    if degrees:
        img = img.rotate(degrees, expand=True)
    if flip:
        img = img.transpose(PIL.Image.FLIP_LEFT_RIGHT)
    if not degrees and not flip:
        transformed = False

    res = {}
    bio = io.BytesIO()
    img.save(bio, format="JPEG")    
    res["from"] = orientation
    res["transformed"] = transformed
    res["stream"] = bio.getvalue()
    return res

def url_join(parts=[]):
    """
    Useful for joining URL fragments to make a single cohesive URL, i.e. for searching. 
    You can see several examples of its use in the `connection.Connection` class.
    """
    parts = [i.strip("/") for i in parts] 
    url = "/".join(parts)
    return url

def get_record_id(rec):
    """
    Extracts the most suitable identifier from a JSON-serialized record on the ENCODE Portal.
    This is useful, for example, for other applications that need to store identifiers of specific 
    records on the Portal. The identifier chosen is determined to be the 'accession' if that 
    property is present, otherwise it's the first alias of the 'aliases' property is present, 
    otherwise its the value of the 'uuid' property.

    Args:
        rec: `dict`. The JSON-serialization of a record on the ENCODE Portal.

    Returns:
        `str`: The extracted record identifier.

    Raises:
        `Exception`: An identifier could not be extracted from the input record.
    """
    if "accession" in rec:
        return rec["accession"]
    elif "aliases" in rec:
        return rec["aliases"][0]
    elif "uuid" in rec:
        return rec["uuid"]
    raise Exception("Could not extract an uptream identifier for ENCODE record '{}'.".format(rec))

def err_context(payload, schema):
    """
    Validates the schema instance against the provided JSON schema.

    Args:
        payload: dict.
        schema: dict.

    Returns:
        `None` if there aren't any instance validation errors. Otherwise, a two-item tuple
        where the first item is the main error message; the second is a dictionary-based
        error hash that contains the contextual errors. This latter item may be empty.
    """
    try:
        jsonschema.validate(payload,schema)
    except jsonschema.exceptions.ValidationError as err:
        main_msg = err.message
        messages = []
        schema_paths = []
        for i in err.context:
            messages.append(i.message)
            schema_paths.append(list(i.absolute_schema_path))
        context = {}
        for i in range(len(schema_paths)):
            context["->".join([str(x) for x in schema_paths[i]])] = messages[i]
        return main_msg, context


def calculate_md5sum(file_path):
    """
    Calculates the md5sum for a local file or a S3 URI. If an S3 URI, the md5sum will be set as
    the objects ETag.

    Args:
        file_path: `str`. The path to a local file or an S3 URI, i.e. s3://bucket-name/key.

    Returns:
        `str`: The md5sum.

    Raises:
        `FileNotFoundError`: The given file_path does not exist. 
    """
    if file_path.startswith("s3:"):
        return encode_utils.aws_storage.S3Object(s3_uri=file_path).md5sum()
    m = hashlib.md5()
    # Assume local file
    if not os.path.exists(file_path):
        msg = "File path '{}' does not exist.".format(file_path)
        logging.error(msg)
        raise FileNotFoundError(msg)
    with open(file_path, 'rb') as fh:
        while True:
            chunk = fh.read(2**16)
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()

def calculate_file_size(file_path):
    """
    Calculates the file size in bytes for a local file or a S3 URI.

    Args:
        file_path: `str`. The path to a local file or an S3 URI, i.e. s3://bucket-name/key.

    Returns:
        `int`.

    Raises:
        `FileNotFoundError`: The given file_path does not exist. 
    """
    if file_path.startswith("s3:"):
        return encode_utils.aws_storage.S3Object(s3_uri=file_path).size()
    if not os.path.exists(file_path):
        msg = "File path '{}' does not exist.".format(file_path)
        logging.error(msg)
        raise FileNotFoundError(msg)
    return os.path.getsize(file_path) 

def print_format_dict(dico, indent=2):
    """Formats a dictionary for printing purposes to ease visual inspection.
    Wraps the ``json.dumps()`` function.

    Args:
        indent: `int`. The number of spaces to indent each level of nesting. Passed directly
            to the ``json.dumps()`` function.
    """
    # Could use pprint, but that looks too ugly with dicts due to all the extra spacing.
    return json.dumps(dico, indent=indent, sort_keys=True)


def clean_aliases(aliases):
    """
    Removes unwanted characters from the alias name.
    This function replaces:

      -both '/' and '\\\\' with '_'.
      -# with "", as it is not allowed according to the schema.

    Can be called prior to registering a new alias if you know it may contain such unwanted
    characters. You would then need to update your payload with the new alias to submit.

    Args:
        aliases: `list`. One or more record alias names to submit to the Portal.

    Returns:
        `str`: The cleaned alias.

    Example::
        clean_alias_name("michael-snyder:a/troublesome\alias")
        # Returns michael-snyder:a_troublesome_alias

    """
    new = []
    for alias in aliases:
        new.append(alias.replace("/", "_").replace("\\", "_").replace("#", ""))
    return new


def create_subprocess(cmd, check_retcode=True):
    """Runs a command in a subprocess and checks for any errors.

    Creates a subprocess via a call to ``subprocess.Popen`` with the argument ``shell=True``, and pipes
    stdout and stderr.

    Args:
        cmd: `str`. The command to execute.
        check_retcode: `bool`. When `True`, then a ``subprocess.SubprocessError`` is raised when the
          subprocess returns a non-zero return code.
          The error message will display the command that was executed along with its
          actual return code,  as well as any messages that the subprocess sent to STDOUT and STDERR.
          When `False`, the ``subprocess.Popen`` instance will be returned instead and it is expected
          that the caller will call its ``communicate`` method.

    Returns:
        Two-item tuple containing the subprocess's STDOUT and STDERR streams' content if
        ``check_retcode=True``, otherwise a ``subprocess.Popen`` instance.

    Raises:
        subprocess.SubprocessError: There is a non-zero return code and ``check_retcode=True``.
    """
    popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check_retcode:
        stdout, stderr = popen.communicate()
        stdout = stdout.strip()
        stderr = stderr.strip()
        retcode = popen.returncode
        if retcode:
            # subprocess.SubprocessError was introduced in Python 3.3.
            raise subprocess.SubprocessError(
                ("subprocess command '{cmd}' failed with returncode '{returncode}'.\n\nstdout is:"
                 " '{stdout}'.\n\nstderr is: '{stderr}'.").format(cmd=cmd, returncode=retcode, stdout=stdout, stderr=stderr))
        return stdout, stderr
    else:
        return popen


def strip_alias_prefix(alias):
    """
    Splits `alias` on ':' to strip off any alias prefix. Aliases have a lab-specific prefix with
    ':' delimiting the lab name and the rest of the alias; this delimiter shouldn't appear
    elsewhere in the alias.

    Args:
        alias: `str`. The alias.

    Returns:
        `str`: The alias without the lab prefix.

    Example::

          strip_alias_prefix("michael-snyder:B-167")
          # Returns "B-167"

    """
    return alias.split(":")[-1]


def add_to_set(entries, new):
    """Adds an entry to a list and makes a set for uniqueness before returning the list.

    Args:
        entries: `list`.
        new: (any datatype) The new member to add to the list.

    Returns:
        `list`: A deduplicated list.
    """
    entries.append(new)
    unique_list = list(set(entries))
    return unique_list


def does_lib_replicate_exist(replicates_json, lib_accession,
                             biological_replicate_number=False, technical_replicate_number=False):
    """
    Regarding the replicates on the specified experiment, determines whether any of them belong
    to the specified library.  Optional constraints are the 'biological_replicate_number' and
    the 'technical_replicate_number' props of the replicates.

    Args:
        replicates_json: `list`. The value of the `replicates` property of an Experiment record.
        lib_accession: `str`. The value of a library object's `accession` property.
        biological_replicate_number: int. The biological replicate number.
        technical_replicate_number: int. The technical replicate number.

    Returns:
        `list`: The replicate UUIDs that pass the search constraints.
    """
    biological_replicate_number = int(biological_replicate_number)
    technical_replicate_number = int(technical_replicate_number)
    results = []  # list of replicate UUIDs.
    for rep in replicates_json:
        rep_id = rep["uuid"]
        if not lib_accession == rep["library"]["accession"]:
            continue
        if biological_replicate_number:
            if biological_replicate_number != rep["biological_replicate_number"]:
                continue
        if technical_replicate_number:
            if technical_replicate_number != rep["technical_replicate_number"]:
                continue
        results.append(rep["uuid"])
    return results
