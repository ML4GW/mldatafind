# mldatafind
[`Law`](https://github.com/riga/law) workflows for streamling gravitational wave data discovery for ML applications

## Example
To run the [example configuration](./example.cfg), first build the container to your desired location

```console
export CONTAINER_PATH=/path/to/mldatafind.sif
apptainer build $CONTAINER_PATH apptainer.def
```

Next, the `Fetch` task, which will query science segments and strain data, can be run using local resources

```console
LAW_CONFIG_FILE=./example.cfg uv run law run mldatafind.law.tasks.Fetch --workflow local --local-scheduler --sandbox mldatafind::$CONTAINER_PATH
```

If you're on a machine with condor access like the LDG, the `Fetch` task can also trivially utilize condor resources by setting `--workflow htcondor` 

```console
LAW_CONFIG_FILE=./example.cfg uv run law run mldatafind.law.tasks.Fetch --workflow htcondor --local-scheduler --sandbox mldatafind::$CONTAINER_PATH
```

condor log files will be stored under the `condor_directory` argument of the `Fetch` task
