#!/usr/bin/env python3
################################################################################
# ray_data_load.py
################################################################################
"""
Ray-Data: Loading data from different sources - tutorials
  https://docs.ray.io/en/master/data/loading-data.html#reading-files

Only couple of examples are copied here. There are several more examples
in above url for loading data into Ray using different data sources.
"""

import sys

import pandas as pd
import ray
from utils import parse_args, fnl

###############################################################################
def main():
    """
    Shell to call do_main() with command-line arguments.
    """
    do_main(sys.argv[1:])

###############################################################################
# pylint: disable=too-many-locals
# pylint: disable=too-many-statements
# pylint: disable=too-many-branches
def do_main(args) -> bool:

    """
    Main driver to implement argument processing.
    """
    # Parsed args and extract cmdline flags into local variables
    parsed_args = parse_args(args, 'Ray Data - Scalable Datasets for ML tutorial')
    verbose     = parsed_args.verbose
    dry_run     = parsed_args.dry_run
    do_debug    = parsed_args.debug_script
    dump_flag   = parsed_args.dump_flags

    # Dump args, also shuts-up pylint ...
    if dump_flag:
        print(f'verbose = {verbose}')
        print(f'do_debug = {do_debug}')

    # Create datasets from on-disk files, Python objects, and cloud storage like S3.
    input_data_file = 's3://anonymous@ray-example-data/iris.parquet'
    if verbose:
        print(fnl(), f'Download Ray data in Parquet format from S3 {input_data_file}...')

    # Ray Data Source in CSV format
    if not dry_run:
        rds = ray.data.read_parquet(input_data_file)
        print(rds.schema())

    if verbose:
        print(fnl(), 'Download Ray data from inline Pandas dataframe ...')

    # Ray Data Source from in-line Pandas data frame
    if not dry_run:
        pd_df = pd.DataFrame({
                    "food": ["spam", "ham", "eggs"],
                    "price": [9.34, 5.37, 0.94]
                    })
        rds = ray.data.from_pandas(pd_df)
        print(rds)


###############################################################################
# Start of the script: Execute only if run as a script
###############################################################################
if __name__ == "__main__":
    main()
