# CASDA mosaic cutout tool

Code for producing a mosaic of various cutout targets. Retrieves cutouts using CASDA and uses linmos for mosaicking the outputs.

* Download cutouts from CASDA
* Generate linmos config
* Run linmos (subprocess call)

This code can be run locally on a laptop with Docker installed (only tested with MacOS) or on a Slurm HPC system.

## Help

```
pipeline.py [-h]
            [--name NAME] [--ra RA] [--dec DEC] --radius RADIUS
            [--freq FREQ] [--vel VEL]
            [--filename FILENAME]
            --output OUTPUT --config CONFIG
            [--sbids SBIDS [SBIDS ...]] [--obs_collection OBS_COLLECTION]
            [--url URL] [--query QUERY] [--milkyway]
            [--singularity SINGULARITY]
            [--scratch SCRATCH] [--askapsoft ASKAPSOFT]
            [--account ACCOUNT] [--time TIME] [--mem MEM]
            [--askapsoft_docker ASKAPSOFT_DOCKER]
            [--local]
            [--no_keyring]
            [--verbose]
```

## Usage

Most of the arguments shown in the help are not required to run the program. The essential arguments are

* `--radius` of the cutout region,
* `--ra` and `--dec` **or** `--name` for the centre position of the cutout,
* `--freq` **or** `--vel` for the spectral range of the cutout,
* `--output` for the directory where the cutouts, mosaics and temporary files will be stored, and
* `--config` for CASDA credentials (see below for template).

Some helpful arguments include

* `--filename` to name the output mosaic image and weights file
* `--sbids` to filter for the known SBIDs containing your target

### Performance notes

* this code takes a long time (many hours) to run due to the slow CASDA staging and cutout retrieval step
* Provide the `--sbids` argument where possible so that fewer large files need to be staged
* Use the smallest cutout possible to minimise file size

### Slurm

To run the program on a HPC (Slurm) environment you will require an environment with Python and [Singularity](https://docs.sylabs.io/guides/3.5/user-guide/introduction.html) installed.

First load the singularity module and pull the required ASKAPsoft singularity image.

```
module load singularity/4.1.0-mpi
singularity pull askapsoft.sif docker://csirocass/askapsoft:1.15.0-setonix
```

Then create an `sbatch` script (e.g. `run.sh`). You will load the python and pip modules, and call the pipeline with arguments for the source position, output directory for your files, and the `askapsoft.sif` image:

```
#!/bin/bash

#SBATCH --account=ja3
#SBATCH --time=24:00:00
#SBATCH --mem=16G
module load python/3.11.6
module load py-pip/23.1.2-py3.11.6
pip install --user -r requirements.txt
python3 pipeline.py \
    --ra <RA [deg]> --dec <DEC [deg]> --radius <RADIUS [arcmin]> \
    --vel <VELOCITY RANGE [km/s]> \
    --filename <OBJECT NAME> \
    --config /path/to/casda.ini \
    --output /path/to/output_dir \
    --askapsoft /path/to/askapsoft.sif
```

**NOTE**: For Setonix the correct default modules are provided in the snippet above, but otherwise run `module spider singularity` to find the correct version for your HPC environment

Then to run the program:

```
sbatch run.sh
```

A subsequent job will be submitted for the mosaicking part of this workflow, which will have a different job id to the output of the `sbatch` command.

### Local

If you have [Docker](https://docs.docker.com/desktop/) installed on your local machine you can run this pipeline directly. Clone this repository, setup a python environment, and install the dependencies

```
git clone https://github.com/AusSRC/casda_mosaic_cutouts
source venv/bin/activate
cd casda_mosaic_cutouts
pip install -r requirements.txt
```

And I would still recommend using a script to run the job (so that you can run it as a background process with `nohup` or you will be waiting half a day for CASDA to give you a cutout). Create one with the following:

```
#!/bin/bash

python3 pipeline.py \
    --ra <RA [deg]> --dec <DEC [deg]> --radius <RADIUS [arcmin]> \
    --vel <VELOCITY RANGE [km/s]> \
    --filename <OBJECT NAME> \
    --config /path/to/casda.ini \
    --output /path/to/output_dir \
    --local
```

Then you can run the job locally as a background process with

```
nohup ./run.sh &
```

**NOTE**: If you don't have a keyring (e.g. Apple KeyChain) installed on your machine, and are running this locally, you can provide an additional `--no_keyring` argument which will prompt you for your password. You will enter it into the command line where the code is running. In this case do not submit the job as a background process.

## Configuration

To authenticate with [CASDA](https://data.csiro.au/) you will need to pass your username and password for your [OPAL account](https://opal.atnf.csiro.au/) via a configuration file. Note that the CASDA astroquery library does not seem to accept NEXUS credentials.

A template `casda.ini`:

```
[CASDA]
username = <username>
password = <password>
```

## FAQ

### Known Linux issues

For users running this tool locally on Linux machines using Docker, ensure that there are public write permissions on the output directory. You can do this with `chmod -R 777 <output_dir>`. This is required for the linmos docker container to write output mosaic and weights file. 
