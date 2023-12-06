#!/usr/bin/env python3
################################################################################
# utils.py
# SPDX-License-Identifier: GNU GPL v3.0
################################################################################
"""
Utility method, mainly parsing and other debugging helper methods.
"""

import os
import argparse
from inspect import currentframe

###############################################################################
# Global Variables: Used in multiple places. List here for documentation
###############################################################################

THIS_SCRIPT          = os.path.basename(__file__)
THIS_PKGSRC_DIR      = os.path.dirname(THIS_SCRIPT)

###############################################################################
# Argument Parsing routine
def parse_args(args, description:str = 'Generic description of tool'):
    """
    Command-line argument parser.

    For how-to re-work argument parsing so it's testable.
    """
    # pylint: disable-msg=line-too-long
    # Ref: https://stackoverflow.com/questions/18160078/how-do-you-write-tests-for-the-argparse-portion-of-a-python-module
    # pylint: enable-msg=line-too-long

    # ---------------------------------------------------------------
    # Start of argument parser, with inline examples text
    # Create 'parser' as object of type ArgumentParser
    parser  = argparse.ArgumentParser(description=description,
                                      formatter_class=argparse.RawDescriptionHelpFormatter,
                                      epilog=r'''Examples:

- Basic usage:
    ''' + THIS_SCRIPT + ''' --src-root-dir < source code root-dir >
''')

    # Define arguments supported by this script
    parser.add_argument('--head-node-addr', dest='head_node_addr'
                        , metavar='<string>'
                        , default='10.212.239.203:6379'
                        , help="Head node's IP-address")

    parser.add_argument('--num-nodes', dest='num_nodes'
                        , metavar='<number>>'
                        , default=4
                        , help='Number of nodes in Ray Cluster')

    parser.add_argument('--num-iterations', dest='num_iterations'
                        , metavar='<number>'
                        , default=10
                        , help='Number of iterations to run ...')

    # ======================================================================
    # Debugging support
    parser.add_argument('--verbose', dest='verbose'
                        , action='store_true'
                        , default=False
                        , help='Show verbose progress messages')

    parser.add_argument('--dry-run', dest='dry_run'
                        , action='store_true'
                        , default=False
                        , help='Dry-run; only show info messages')

    parser.add_argument('--debug', dest='debug_script'
                        , action='store_true'
                        , default=False
                        , help='Turn on debugging for script\'s execution')

    parser.add_argument('--dump-data', dest='dump_flags'
                        , action='store_true'
                        , default=False
                        , help='Dump args, other data for debugging')

    parsed_args = parser.parse_args(args)

    if parsed_args is False:
        parser.print_help()

    return parsed_args

# ------------------------------------------------------------------------------
def fnl():
    """
    Return calling function's brief-name and line number
    Ref: https://stackoverflow.com/questions/35701624/pylint-w0212-protected-access
    ... for why we need for use of pylint disable directive.
    """
    curr_fr = currentframe()
    # pylint: disable=protected-access
    fn_name = curr_fr.f_back.f_code.co_name
    # pylint: enable=protected-access
    line_num = curr_fr.f_back.f_lineno
    return fn_name + ':' + str(line_num)
