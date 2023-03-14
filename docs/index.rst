.. IGVF Utils documentation master file, created by
   sphinx-quickstart on Thu Jan 18 20:14:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

IGVF Utils package
========================================

Installation_ and configuration_ instructions are provided on the project's `GitHub wiki`_.

.. _installation: https://github.com/IGVF-DACC/igvf_utils/wiki/Installation

.. _configuration: https://github.com/IGVF-DACC/igvf_utils/wiki/Configuration

.. _GitHub wiki: https://github.com/IGVF-DACC/igvf_utils/wiki


Examples
--------

.. toctree::
   :maxdepth: 2
 
   Connection class <examples>
   Transferring files to GCP <examples_gcp>

.. :doc:`examples`

Scripts
-------

.. toctree::
    :maxdepth: 1

    registration <scripts/iu_register>
    scripts/iu_check_not_posted.rst
    scripts/iu_create_gcp_url_list.rst
    scripts/iu_generate_upload_creds.rst
    scripts/iu_get_aliases.rst
    scripts/iu_get_accessions.rst

Client API Modules
------------------

.. toctree::
   :maxdepth: 3

   aws_storage
   connection
   igvf_utils
   profiles 
   utils

Unit Tests
----------

.. toctree::
   :maxdepth: 3

   tests/test_utils
   tests/test_connection

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
