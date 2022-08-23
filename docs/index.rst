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
   
    registration <scripts/eu_register>
    scripts/eu_add_controlled_by.rst
    scripts/eu_check_not_posted.rst
    scripts/eu_create_gcp_url_list.rst
    scripts/eu_generate_upload_creds.rst
    scripts/eu_get_aliases.rst
    scripts/eu_get_accessions.rst
    scripts/eu_get_replicate_fastq_encffs.rst
    scripts/eu_get_replicate_numbers.rst
    scripts/eu_report_fastq_content_errors.rst
    scripts/eu_s3_to_gcp.rst

Client API Modules
------------------

.. toctree::
   :maxdepth: 3

   aws_storage
   connection
   igvf_utils
   profiles 
   transfer_to_gcp 
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
