.. ENCODE Utils documentation master file, created by
   sphinx-quickstart on Thu Jan 18 20:14:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ENCODE Utils package
========================================

Installation_ and configuration_ instructions are provided on the project's `GitHub wiki`_.

.. _installation: https://github.com/StanfordBioinformatics/encode_utils/wiki/Installation

.. _configuration: https://github.com/StanfordBioinformatics/encode_utils/wiki/Configuration

.. _GitHub wiki: https://github.com/StanfordBioinformatics/encode_utils/wiki


Examples
--------

.. toctree::
   :maxdepth: 2
 
   Connection class <examples>

.. :doc:`examples`

Scripts
-------

.. toctree::
    :maxdepth: 1
   
    registration <scripts/eu_register>
    scripts/eu_add_controlled_by.rst
    scripts/eu_check_not_posted.rst
    scripts/eu_get_aliases.rst
    scripts/eu_get_replicate_fastq_encffs.rst
    scripts/eu_get_replicate_numbers.rst
    scripts/eu_report_fastq_content_errors.rst
    scripts/eu_s3_to_gcp.rst

Client API Modules
------------------

.. toctree::
   :maxdepth: 3

   transfer_to_gcp 
   encode_utils
   utils
   connection
   profiles 

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
