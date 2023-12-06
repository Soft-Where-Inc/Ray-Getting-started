#!/usr/bin/env python3
################################################################################
# ray_crash_course_tasks_pll.py
################################################################################
"""
Ray Crash Course: Tasks and remote execution.

In this example, if a Ray cluster is already up, ray_init() will connect to it.
Otherwise, it will start, something called, a local cluster.

To start a cluster manually:

    $ ray start --head --port=6379 --object-manager-port=8076 --dashboard-host=0.0.0.0

    # Cut-n-paste below, the address reported by above command.
    $ ray start --address='10.212.239.203:6379' # Repeat n-times

On Mac/OSX, start both head-node and worker nodes using:
    RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1

  - Monitor Ray nodes

# pylint: disable=line-too-long
  $ RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1 ray start --head --port=6379 --object-manager-port=8076 --include-dashboard=True

  $ RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1 ray start --address='192.168.7.211:6379'

  # Will return descriptive information about the nodes
  $ ray list nodes

Refs: https://github.com/anyscale/academy/blob/ebd151134127168162c1175ed1c7da979e475a83/ray-crash-course/01-Ray-Tasks.ipynb
# pylint: enable=line-too-long
"""

import sys
import ray
from ray.util.state import list_nodes, get_node
from utils import parse_args, fnl

###############################################################################
def regular_function() -> str:
    """Return function's code-location"""
    return fnl()

# A Ray remote function.
@ray.remote
def remote_function() -> str:
    """Return constant value"""

    # Will return a list describing nodes
    # return list_nodes()

    # Get info about specific node, given its node-ID
    return get_node('9aa6d056371a91c133fdce8d4a20b458bbfac6eff733872478f328a0')
    # return 4

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
    parsed_args    = parse_args(args, 'Ray - Remote execution of a method')
    head_node_addr = parsed_args.head_node_addr
    verbose        = parsed_args.verbose
    dry_run        = parsed_args.dry_run
    do_debug       = parsed_args.debug_script
    dump_flag      = parsed_args.dump_flags

    # Dump args, also shuts-up pylint ...
    if dump_flag:
        print(f'verbose = {verbose}')
        print(f'do_debug = {do_debug}')

    if not dry_run:
        print('Return from regular_function:' + regular_function())
        ray.init(address=head_node_addr)

        print('\nRay list_nodes: ', list_nodes())
        print('\n')

        # Let's invoke the remote regular function.
        remf_rv = ray.get(remote_function.remote())
        print(f'Return from remote function invocation {remf_rv}')

###############################################################################
# Start of the script: Execute only if run as a script
###############################################################################
if __name__ == "__main__":
    main()
