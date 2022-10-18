# mldatafind
Gravitational wave data discovery tools for ML applications

## Installation

Make sure the conda dependencies in the `environment.yaml` are installed, e.g. in a conda environment

```
conda env create -f environment.yaml
```

Then, this project can be installed via [Poetry](https://python-poetry.org/) by adding it as a local dependency

``` toml
[tool.poetry.dependencies]
python = "^3.8"  # python versions 3.8-3.10 are supported
mldatafind = {path = "path/to/mldatafind", develop = true}
```

You can then update your lockfile/environment with

```
poetry update
```

Alternatively, if you have the [pinto](github.com/ML4GW/pinto/) environment management tool installed, you can run

```
pinto build
```
in the project home directory. This will install both the conda and poetry dependencies in one swoop!
