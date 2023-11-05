#!/usr/bin/env python3
################################################################################
# ray_data_scalable_datasets_for_ml.py
################################################################################
"""
Ray-Data: Scalable Datasets for ML - tutorial exercise.
"""

import sys

from typing import Dict
import numpy as np
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
    if verbose:
        print(fnl(), 'Download Ray data in CSV-format from S3 ...')

    # Ray Data Source in CSV format
    if not dry_run:
        rds_csv = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

        # Apply functions to transform data.
        # Ray Data executes transformations in parallel.
        transformed_ds = rds_csv.map_batches(compute_area)

        # Iterate over batches of data.
        for one_batch in transformed_ds.iter_batches(batch_size=4):
            print(one_batch)

    # Save dataset contents to on-disk files or cloud storage.
    outfile = 'local:///tmp/iris/'
    save_verb = 'Saved'
    if dry_run:
        save_verb = 'Save'
    else:
        transformed_ds.write_parquet(outfile)
    print(fnl(), f"{save_verb} data set contents to output file: '{outfile}'")

################################################################################
def compute_area(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """Compute the area given a batch of NumPy-arrays."""
    length = batch["petal length (cm)"]
    width = batch["petal width (cm)"]
    batch["petal area (cm^2)"] = length * width
    return batch

###############################################################################
# Start of the script: Execute only if run as a script
###############################################################################
if __name__ == "__main__":
    main()
