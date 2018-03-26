#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""Sets the `File.controlled_by` property for all FASTQ File records on an Experiment.

An experiment can have multiple controls. This script will only work if:

  1. The `Experiment.possible_controls` is already set, and
  2. If more than one control, all controls have the same number of biologoical replicates, and
  3. The number of biological replicates on the Experiment is the same as the number of biological
     replicates for any given control.

Having varying number of technical replicates is fine.

These requiements are set in order to have some meaningful, automated way of assigning control
FASTQ files to experimental FASTQ files.

The algorithm works as follows:

  1. The biological replicate numbers on the experiment are stored in a `list`, and ordered from least to greatest.
  2. The biological replicate numbers on each control are stored in a `list`, and ordered from least to greatest.
  3. Biological Replicate Assignment: For each control, its biological replicate number `list` created in
     step 2 is superimposed onto the `list` created in step 1. Assignment then takes place by
     positional matching. The matching is positional instead of simply by biological replicate number
     itself, since the numbering is known in some cases to not alwasy be sequential starting from 1.
  4. Setting `possible_controls`: For each replicate in the Experiment, the forward reads FASTQ file
     linked to that replicate is assigned to the forward reads control FASTQ files. The control FASTQs
     are determined from the superimposition step above. For each control, all replicates that have
     the same `biologican_replicate_number` are included. If the sequencing is paired-end, then
     the same type of assignment happens for the reverse reads files.
"""

import argparse

from encode_utils.connection import Connection
from encode_utils.parent_argparser import dcc_login_parser
# dcc_login_parser  contains the arguments needed for logging in to the
# ENCODE Portal, including which env.


def get_parser():
    parser = argparse.ArgumentParser(
        parents=[dcc_login_parser],
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-e", "--exp-id", required=True, help="""
    An identifier of an Experiment record whose FASTQ File objects need to have their
    `controlled_by` property set.""")

    return parser


import pdb


def main():
    """Program
    """
    parser = get_parser()
    args = parser.parse_args()
    mode = args.dcc_mode
    exp_id = args.exp_id

    conn = Connection(mode)
    exp_rep_dico = conn.get_fastqfile_replicate_hash(exp_id)
    exp_json = conn.get(exp_id, ignore404=True)
    controls = exp_json["possible_controls"]  # A list of dicts.

    # Populate a controls-lookup hash. The keys will be the ctl accessions. Each value will be
    # the replicates hash (return value of conn.get_fastqfile_replicate_hash().
    controls_hash = {}  # A dict of dicts.
    control_bio_rep_counts = []
    for c in controls:
        ctl_accession = c["accession"]
        controls_hash[ctl_accession] = {}
        ctl_rep_dico = conn.get_fastqfile_replicate_hash(ctl_accession)
        controls_hash[ctl_accession]["rep_dico"] = ctl_rep_dico
        control_bio_rep_counts.append(len(ctl_rep_dico.keys()))

    # Make sure that all control experiments have the same number of biological replicates. There are
    # no known rules to apply otherwise.
    if len(set(control_bio_rep_counts)) != 1:
        raise Exception(
            "The controls '{controls}' have different numbers of biological replicates from one another '{rep_nums}'.".format(
                controls=control_ids,
                rep_nums=control_bio_rep_counts))

    # Make sure that the number of control bio reps equals the number of experiment bio reps:
    exp_bio_rep_count = len(exp_rep_dico.keys())
    if exp_bio_rep_count != control_bio_rep_counts[0]:
        raise Exception(
            "The number of experiment replicates '{}' doesn't equal the number of control replicates '{}'.".format(
                exp_bio_rep_count, control_bio_rep_counts[0]))

    # Now we'll look at each bio rep on the experiment, in numerical order of
    # biological_replicate_number from least to greatest. We'll work our way all the down to the
    # FASTQ files and start populating the File.controlled_by property in the following manner:
    #
    #  For each control, we'll sort the replicates the same was as we did for the ones on the
    #  experiment, then for the replicate having the same ordinal index, we'll add the FASTQ File
    #  references.

    sorted_exp_bio_reps = sorted(exp_rep_dico)
    count = -1
    # And now for the nastiest for-loop I've ever written ... this should be cleaned up but the logic
    # is so rough to implement that it'll be ugly any way we look at it.
    for b in sorted_exp_bio_reps:  # biological_replicate_number
        count += 1
        for t in exp_rep_dico[b]:  # technical_replicate_number
            for read_num in exp_rep_dico[b][t]:
                for fastq_json in exp_rep_dico[b][t][read_num]:
                    exp_file_acc = fastq_json["accession"]
                    controlled_by = []
                    for c in controls_hash:
                        ctl_bio_rep_num = sorted(controls_hash[c]["rep_dico"])[count]
                        ctl_tech_reps = controls_hash[c]["rep_dico"][ctl_bio_rep_num]
                        for ctl_tech_rep_num in ctl_tech_reps:
                            for ctl_encff in ctl_tech_reps[ctl_tech_rep_num][read_num]:
                                controlled_by.append(ctl_encff["accession"])
                    conn.patch({conn.ENCID_KEY: exp_file_acc,
                                "controlled_by": controlled_by},
                               extend_array_values=False)
                    #print({conn.ENCID_KEY: exp_file_acc, "controlled_by": controlled_by})


if __name__ == "__main__":
    main()
