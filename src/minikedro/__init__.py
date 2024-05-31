__version__ = "0.0.1"
"""minikedro
100 lines of code
Support:
1. Run a pipeline (a series of functions)
2. Run a pipeline partially (some filtering mechanism)
3. Load data in a declarative way
4. Easy to extend (i.e. extra logging)
5. Load configuration stored in YAML
6. Some template value feature (i.e. shared config using the same S3 bucket)
7. Load data remotely


1. Step I: Run a series of functions to process some data.


Things that are not implemented:
- Kedro Environment
- Credentials

"""


class DataCatalog: ...


class ConfigLoader: ...


class Pipeline: ...


class Node:  # Maybe not needed
    ...


class Hook:  # Maybe not needed
    ...
