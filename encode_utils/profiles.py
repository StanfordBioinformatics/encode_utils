# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Contains a ``Profile`` class for working with profiles on the ENCODE Portal.  Note that
the terms 'profile' and 'schema' are used interchangeably in this package.
"""

import inflection
import logging
import requests

import encode_utils as eu
import encode_utils.utils as euu


#: A debug ``logging`` instance.
DEBUG_LOGGER = logging.getLogger(eu.DEBUG_LOGGER_NAME + "." + __name__)
#: An error ``logging`` instance.
ERROR_LOGGER = logging.getLogger(eu.ERROR_LOGGER_NAME + "." + __name__)


class UnknownProfile(Exception):
    """
    Raised when the profile ID in question doesn't match any known profile ID.
    """
    pass


class EncodeSchema:
    def __init__(self, name, schema):
        """
        Args:
            name: `str`. The name of the schema, should be lowercase and `-`-separated.
            schema: `dict`. The JSON representation of the schema on the portal.
        """
        self.name = name
        self.schema = schema
        self._properties = None
        self._non_writable_props = None
        self._writable_props = None

    @property
    def properties(self):
        """
        Returns:
            `list[EncodeSchemaProperty]`: A list of properties in the schema
        """
        if self._properties is None:
            props = []
            for prop_name, prop in self.schema["properties"].items():
                is_identifying = prop_name in self.identifying_properties
                is_required = prop_name in self.required_properties
                props.append(
                    EncodeSchemaProperty(prop_name, prop, is_required, is_identifying)
                )
            self._properties = props
        return self._properties

    def get_property_from_name(self, name):
        """
        Args:
            name: `str`. The name of the property to search for.
        Returns:
            `EncodeSchemaProperty`: The property corresponding to `name`
        Raises:
            `ValueError` if a property with `name` is not found.
        """
        for prop in self.properties:
            if prop.name == name:
                return prop
        raise ValueError("Could not find property {} in schema".format(name))

    @property
    def identifying_properties(self):
        """
        Returns:
            `list`: A list of identifying property names
        """
        return self.schema["identifyingProperties"]

    @property
    def has_award(self):
        """
        Returns:
            `bool`: Indicates if the schema has an `award` property present.
        """
        return eu.AWARD_PROP_NAME in self.schema

    @property
    def has_alias(self):
        """
        Returns:
            `bool`: Indicates if the schema has an `alias` property present.
        """
        return eu.ALIAS_PROP_NAME in self.schema

    @property
    def non_writable_props(self):
        """
        A list of the property names that are non-writable. These are determined as
        properties in the schema whose subschemas include the property
        ``Profile.NOT_SUBMITTABLE_FLAG`` or the property ``Profile.READ_ONLY_FLAG``.
        """
        if self._non_writable_props is None:
            for prop in self.properties:
                if prop.is_not_submittable or prop.is_read_only:
                    self.non_writable_props.append(prop.name)
        return self._non_writable_props

    @property
    def writable_props(self):
        """
        A list of the property names that are writable, which are those that don't
        fall into the self.non_writable_props category.
        """
        if self._writable_props is None:
            for prop in self.properties:
                if not prop.is_not_submittable and not prop.is_read_only:
                    self.writable_props.append(prop.name)
        return self._writable_props

    @property
    def required_properties(self):
        """
        Returns the list of required properties to submit when creating a new record
        under the given profile. Only works when the profile contains a "required" key
        at the top level, as it is in the biosample profile. Doesn't at this time
        recognize conditionally required keys that appear in a subschema, such as
        'anyOf' as demonstrated in the file profile.

        Returns:
            `list`: The list of required properties.
        """
        return self.schema.get("required", [])

    def filter_non_writable_props(self, rec_json, keep_identifying=False):
        """
        Filters out the non-writable properties from a record, using
        ``self.non_writable_props`` as a filtering basis.

        Args:
            rec_json: `dict`. The JSON serialization of a record that belongs to the
              profile encapsulated through this instance.
            keep_identifying: `bool`. Setting this to True means to retain keys that are
              in the `identifyingProperties` object property of the schema.
        Returns:
            `dict`: The input minus any keys that aren't writable.
        """
        for key in rec_json.keys():
            prop = self.get_property_from_name(key)
            if keep_identifying and prop.is_identifying:
                continue
            if key in self.non_writable_props:
                rec_json.pop(key)
        return rec_json


class EncodeSchemaProperty:
    #: Constant storing the name of the property in a JSON object sub-schema that
    #:  indicates whether the object is read only.
    READ_ONLY_FLAG = "readonly"
    #: Constant storing the name of the property in a JSON object sub-schema that
    #: indicates whether the object is submittable.
    NOT_SUBMITTABLE_FLAG = "notSubmittable"

    def __init__(self, prop_name, schema, is_required, is_identifying):
        """
        Indicates whether the provided property name is one that a user can submit when
        creating or updating a record.

        Args:
            prop_name: `str`. The name of the property in the schema
            prop: `str`. The `dict` from the portal JSON schema defining the property
            is_required: `bool`. Indicates whether or not the given property is required
            is_identifying: `bool`. Indicates whether or not the given property is
              identifying.
        """
        self.name = prop_name
        self.schema = schema
        self.is_required = is_required
        self.is_identifying = is_identifying

    def is_not_submittable(self):
        """
        Indicates whether the provided property name is one that a user can submit when
        creating or updating a record.

        Returns:
            `bool`: `True` if this is a non-submittable property, `False` otherwise.
        """
        if self.NOT_SUBMITTABLE_FLAG in self.schema:
            return True
        return False

    def is_read_only(self):
        """
        Indicates whether the provided property name is one that is read-only and hence
        can't be modified by the end-user.

        Returns:
            `bool`: `True` if this is a read-only property, `False` otherwise.
        """
        if self.READ_ONLY_FLAG in self.schema:
            return True
        return False


class Profiles:
    """
    Encapsulates knowledge about the existing profiles on the Portal and contains useful
    methods for working with a given profile.

    A defining purpose of this class is to validate the profile ID specified in a POST
    payload passed to ``encode_utils.connection.Connection.post()``.  This class is used
    to ensure that the profile specified there is a known profile on the Portal.

    Args:
        dcc_url: str. The portal URL being submitted to.
    """
    #: Constant storing the `file.json` profile's ID.
    #: This is asserted for inclusion in ``Profile.PROFILES``.
    FILE_PROFILE_ID = "file"

    #: Constant storing a property name of the `file.json` profile.
    #: The stored name is asserted for inclusion in the set of `File` properties.
    SUBMITTED_FILE_PROP_NAME = "submitted_file_name"

    #: Constant storing a property name of the `file.json` profile.
    #: The stored name is asserted for inclusion in the set of `File` properties.
    MD5SUM_NAME_PROP_NAME = "md5sum"
    #: Constant sotring a property name of the `file.json` profile.
    FILE_SIZE_PROP_NAME = "file_size"

    def __init__(self, dcc_url):
        """
        Args:
            dcc_url: `str`. The dcc_url as specified by Connection.dcc_url.
        """
        self.dcc_url = dcc_url
        self._profiles = None

    def _get_profiles(self):
        """
        Creates a dictionary storing all public profiles on the Portal.

        Returns:
            `dict`: `dict` where each key is the profile's ID, and each value is a given
            profile's JSON schema.  Each key is extracted from the profile's `id`
            property, after a little formatting first.  The formatting works by removing
            the '/profiles/' prefix and the '.json' suffix.  For example, the value of
            the `id` property for the `genetic_modification.json` profile is
            `/profiles/genetic_modification.json`. The corresponding key in this `dict`
            is `genetic_modification`.
        """
        url = euu.url_join([self.dcc_url, eu.PROFILES_URL, "?format=json"])
        profiles = requests.get(url,
                                timeout=eu.TIMEOUT,
                                headers=euu.REQUEST_HEADERS_JSON).json()
        # Remove the "private" profiles, since these have differing semantics.
        private_profiles = [x for x in profiles if x.startswith("_")]  # i.e. _subtypes
        for i in private_profiles:
            # _subtypes should be the only one
            profiles.pop(i)
        if "@type" in profiles:  # A pseudo profile that doesn't count.
            profiles.pop("@type")

        profile_id_hash = {}  # Instead of name as key, profile ID is key.
        for schema in profiles.values():  # i.e. name=GeneticModification
            profile_id = schema["id"].split("/")[-1].split(".json")[0]
            profile_id_hash[profile_id] = EncodeSchema(profile_id, schema)
        return profile_id_hash

    @property
    def profiles(self):
        """
        Constant (`dict`) set to the return value of the function
        ``self.get_profiles()``. See documentation there for details.
        """
        if self._profiles is None:
            self._profiles = self._get_profiles()
        return self._profiles

    def profiles_with_property(self, property_name):
        """
        Returns a list of profile names that have a given property.

        Args:
            property_name: `str`. The name of the property.

        Returns:
            `list` of profile names.
        """
        res = []
        for profile_name in self.profiles:
            if property_name in self.profiles[profile_name]["properties"]:
                res.append(profile_name)
        return res

    def get_profile_from_id(self, at_id):
        """
        Normalizes the `profile_id` so that it matches the format of the profile IDs
        stored in ``self.profiles``, and ensures that the normalized profile ID is a
        member of this list.

        Args:
            at_id: `str`. An `@id` from the portal, e.g. `/biosamples/ENCBS123ABC/`

        Returns:
            `str`: The normalized profile ID.
        Raises:
            UnknownProfile: The normalized profile ID is not a member of the list
            `self.profiles`.
        """
        profile_id = at_id.strip("/").split("/")[0].lower()
        # Multi-word profile names are hypen-separated, i.e. genetic-modifications.
        profile_id = profile_id.replace("-", "_")
        profile_id = inflection.singularize(profile_id)
        # There are some notable cases where the profile ID doesn't match what is used
        # in a record's @id attribute. For example, the profile antibody_lot has records
        # whose @id property looks like '/antibodies/ENCAB719MQZ' instead of the
        # expected '/antibody_lots/ENCAB719MQZ'. The block below fixes such exceptions:
        if profile_id == "antibody":
            profile_id = "antibody_lot"
        if profile_id == "publication_datum":
            profile_id = "publication_data"

        if profile_id not in self.profiles:
            raise UnknownProfile("Unknown profile ID '{}'.".format(at_id))
        return self.profiles[profile_id]

    def remove_duplicate_associations(self, associations):
        """
        Checks for duplicates in array properties containing string elements. Need to be
        careful as some cases can be tricky, i.e.

            ['/documents/id1', 'id1']

        Such a duplicate should be identified and removed, leaving us with ["id1"].

        Args:
            associations: `list`.

        Returns:
            Deduplicated `list`.
        """
        for i in range(len(associations)):
            val = associations[i]
            if val.startswith("/"):
                prefix = inflection.singularize(val.strip("/").split("/")[0])
                if prefix in self.profiles:
                    associations[i] = val.strip("/").split("/")[-1]
        return list(set(associations))
