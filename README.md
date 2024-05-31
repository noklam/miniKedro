# minikedro

A functional minimalistic of implementation of Kedro (inspired by minGPT).

## Structure
The repository is a clone of the standard spaceflights repository with two additional files:
- run_v1.py
- minikedro.v1.py

In each step, you would run different versions of `run_vx.py` and it will use the class defined in `minikedro.vx.py`. For example, there is no changes in class from v4 to v5 in `minikedro`, thus you will only find `v4` folder but no `v5` folder.

Each step I try to refactor part of the script and introduce a new component or a new feature for existing component. By the end you will get something resemble Kedro with a basic set of feature.

There is also a `dataset.ipynb` to demo how dataset works (out of scope)

# How to use this repository
Run `python run.py` and the following scripts and observe the difference between each versions.

## run.py
The basic structure of a script without any Kedro

## run_v1.py
- Add `ConfigLoader` and extract configuration to a dictionary.

## run_v2.py
- Replace shared config with template value `${_base_folder}` with enhanced `ConfigLoader`

## run_v3.py
- `dataset.ipynb` to explain how `kedro-datasets` works and demonstrate the I/O capability (`save` and `load` interface)
- Add `DataCatalog` with the ability to `load`

## run_v4.py
- Add `save` to `DataCatalog` and actually save data in the script as well.

## run_v5.py
- Extract Configuration to a `steps` as a list of dictionary

## run_v6.py
- Make use of the `steps` variable and loop through the steps.

## run_v7.py
- Introduce a thin `list` wrapper called `pipeline` and `step` wrapper called `nodes`
- Rename `steps` as `nodes`, `step` as `node_`

## run_v8.py
- Replace `pipeline` from `kedro` with `minikedro.v7`
- Replace the configuration dictionary and load it from `base/conf/catalog.yml` instead.

Lines of code:
- `run_v8.py`: 41
- `minikedro.v7.__init__.py`: 38

## run_v9.py (Bonus)
- Introduce Hooks