# Ray-Getting-started
Python exercises to get started with Ray, using online training exercises.

We use this directory to download Python tutorials as found in the link
below and run them through pylint and the dev environment on my personal Mac.

## References

- Ray [Getting started](https://docs.ray.io/en/master/ray-overview/getting-started.html) tutorials

To get these programs working, in addition to the install steps documented
above, we need to install the following packages

- python3.11 -m pip install --upgrade pip
- pip install -U "ray[data]"

  # Need this to run dashboard related interfaces
- pip install -U "ray[default]"

# Needed by ray_data_scalable_datasets_for_ml.py, ray_data_load.py ... others, too
- pip install grpc
- pip install grpcio

# Needed to run `ray list nodes` and other dashboard-related commands
- pip install async_timeout
