class AwardPropertyMissing(Exception):
    """
    Raised when the `award` property isn't set in the payload when doing a POST, and a
    default isn't set by the environment variable `DCC_AWARD` either.
    """
    message = (
        "The property '{}' is missing from the payload and a default isn't set either. "
        "To store a default, set the DCC_AWARD environment variable."
    )


class FileUploadFailed(Exception):
    """
    Raised when the AWS CLI returns a non-zero exit status.
    """


class MissingAlias(Exception):
    """
    Raised when POSTING a payload that doesn't contain the 'aliases' property and the
    argument require_aliases in Connection.post() is set to False.
    """


class LabPropertyMissing(Exception):
    """
    Raised when the `lab` property isn't set in the payload when doing a POST, and a
    default isn't set by the environment variable `DCC_LAB` either.
    """
    message = (
        "The property '{}' is missing from the payload and a default isn't set either. "
        "To store a default, set the DCC_LAB environment variable."
    )


class ProfileNotSpecified(Exception):
    """
    Raised when the profile (object schema) to submit to isn't specifed in a POST
    payload.
    """
    pass


class RecordIdNotPresent(Exception):
    pass


class RecordNotFound(Exception):
    """
    Raised when a record that should exist on the Portal can't be retrieved via a GET
    request.
    """
    pass


class S3ToGCPFailed(Exception):
    """
    Raised when a file from an ENCODE S3 bucket fails to copy to a GCP bucket path.
    """
    pass
