#!/usr/bin/env python3
################################################################################
# basic_remote_execution.py
################################################################################
"""
Ray Basic remote execution of a method in a cluster.

In this example, if a Ray cluster is already up, ray_init() will connect to it.
Otherwise, it will start, something called, a local cluster.

To start a cluster manually:

    $ ray start --head --port=6379 --object-manager-port=8076 --dashboard-host=0.0.0.0

    # Cut-n-paste below, the address reported by above command.
    $ ray start --address='10.212.239.203:6379' # Repeat n-times
"""

import sys
import ray
from utils import parse_args, fnl

###############################################################################
@ray.remote
def fsquare(num:int) -> int:
    """Fn to compute and return the square of a number"""
    return num * num

###############################################################################
def main():
    """
    Shell to call do_main() with command-line arguments.
    """
    do_main(sys.argv[1:])

###############################################################################
def do_main(args) -> bool:
    """
    Main driver to implement argument processing.
    """
    # Parsed args and extract cmdline flags into local variables
    parsed_args = parse_args(args, 'Ray - Remote execution of a method')
    verbose     = parsed_args.verbose
    num_iters   = parsed_args.num_iterations
    dry_run     = parsed_args.dry_run
    do_debug    = parsed_args.debug_script
    dump_flag   = parsed_args.dump_flags

    # Dump args, also shuts-up pylint ...
    if dump_flag:
        print(f'verbose = {verbose}')
        print(f'do_debug = {do_debug}')

    if not dry_run:
        ray.init()
        futures = [fsquare.remote(i) for i in range(num_iters)]
        print(fnl(), ray.get(futures)) # [0, 1, 4, 9, ...]

###############################################################################
# Start of the script: Execute only if run as a script
###############################################################################
if __name__ == "__main__":
    main()
