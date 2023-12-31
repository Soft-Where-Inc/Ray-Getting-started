#!/usr/bin/env python3
################################################################################
# ray_cluster_quickstart.py
################################################################################
"""
Ray Cluster: Simple quick-start example program to start a Ray Cluster
  https://docs.ray.io/en/master/data/loading-data.html#reading-files
  https://docs.ray.io/en/master/ray-overview/getting-started.html

Ray Cluster Quickstart

To start a cluster manually:

    $ ray start --head --port=6379 --object-manager-port=8076 --dashboard-host=0.0.0.0

    # Cut-n-paste below, the address reported by above command.
    $ ray start --address='10.212.239.203:6379' # Repeat n-times

On Mac/OSX, start both head-node and worker nodes using:
    RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
"""

import sys
import platform
import time
from collections import Counter
import ray
from utils import parse_args, fnl

###############################################################################
@ray.remote
def get_host_name(host_name):
    """Sample method to return host name after a small amount of sleep."""

    time.sleep(0.01)
    return host_name + (platform.node(),)

def wait_for_nodes(expected):
    """ Wait for all nodes to join the cluster."""
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print(fnl(),
                  f'{num_nodes} nodes have joined so far, waiting for {expected - num_nodes} more.')
            sys.stdout.flush()
            time.sleep(5)
        else:
            print(fnl(), f'{num_nodes} nodes have joined the cluster.')
            break

###############################################################################
def main():
    """
    Shell to call do_main() with command-line arguments.
    """
    do_main(sys.argv[1:])

###############################################################################
# pylint: disable=too-many-branches
def do_main(args) -> bool:

    """
    Main driver to implement argument processing.
    """
    # Parsed args and extract cmdline flags into local variables
    parsed_args    = parse_args(args, 'Ray Cluster - Quick start tutorial')
    num_nodes      = parsed_args.num_nodes
    head_node_addr = parsed_args.head_node_addr
    num_iters      = parsed_args.num_iterations
    verbose        = parsed_args.verbose
    dry_run        = parsed_args.dry_run
    do_debug       = parsed_args.debug_script
    dump_flag      = parsed_args.dump_flags

    # Dump args, also shuts-up pylint ...
    if dump_flag:
        print(f'verbose = {verbose}')
        print(f'do_debug = {do_debug}')

    # Either one seems to work ok to connect to the cluster.
    # head_node_addr='localhost:6379'
    if verbose:
        print(f"Initialize Ray Cluster with head node at '{head_node_addr}'")

    if dry_run is False:
        ray.init(address=head_node_addr)
        wait_for_nodes(num_nodes)

    print('Ray Cluster Info: ', ray.cluster_resources())

    print('Platform Node name: ', platform.node())
    # print(get_host_name.remote(platform.node()))
    try:
        print(get_host_name('abc'))
    except TypeError:
        print('\n',fnl(), '**** Error: Cannot call remote methods directly. '
                        + 'Need to invoke them using .remote notation.\n')

    # The remote execution returns an object ...
    print(fnl(), get_host_name.remote(get_host_name.remote(())))

    # And you have to call get() on this futures object to retrieve the result
    print(fnl(), ray.get(get_host_name.remote(get_host_name.remote(()))))

    # This errors out; call with RAY_IGNORE_UNHANDLED_ERRORS=1 ...
    # print(fnl(), get_host_name.remote('abc-remote'))

    # Even with that RAY_IGNORE_UNHANDLED_ERRORS=1, we still get a TypeError exception ...
    # print(fnl(), ray.get(get_host_name.remote('abc-remote')))

    # sys.exit(0)

    # Check that objects can be transferred from each node to each other node.
    for i in range(num_iters):
        # print(f'Iteration {i}')

        # This causes a TypeError, see similar issue few lines above ...
        # print(fnl(), f'Iteration {i}', get_host_name.remote(platform.node()))

        results = [get_host_name.remote(get_host_name.remote(())) for _ in range(100)]
        print(f'Iteration {i}', Counter(ray.get(results)))
        # print(Counter(ray.get(results)))
        sys.stdout.flush()

    print("Success!")
    sys.stdout.flush()

    sleep_for_s=20
    if verbose:
        print(f'Sleep for {sleep_for_s} seconds to allow cluster to shutdown')

    time.sleep(sleep_for_s)

###############################################################################
# Start of the script: Execute only if run as a script
###############################################################################
if __name__ == "__main__":
    main()
