# Mosaic cutout tool

Code for producing mosaics of target sources. Uses CASDA for generating the cutouts and linmos for mosaicking the outputs.

* Download cutouts from CASDA
* Generate linmos config
* Run linmos (subprocess call)

## Help

```
usage: pipeline.py [-h]
                   --radius RADIUS
                   [--name NAME] [--ra RA] [--dec DEC]
                   [--freq FREQ] [--vel VEL]
                   --output OUTPUT --config CONFIG
                   [--obs_collection OBS_COLLECTION] [--milkyway]
                   [--url URL] [--query QUERY]
                   [--filename FILENAME]
                   [--singularity SINGULARITY]
                   [--scratch SCRATCH] [--askapsoft ASKAPSOFT]
                   [--account ACCOUNT] [--time TIME] [--mem MEM]
                   [--verbose]
```

## Usage

This program needs to be run on a HPC (Slurm) environment with Python and [Singularity](https://docs.sylabs.io/guides/3.5/user-guide/introduction.html) installed.

Most of the arguments shown in the help are not required to run the program. The essential arguments are

* `--radius` of the cutout region,
* `--ra` and `--dec` or `--name` for the centre position of the cutout,
* `--freq` or `--vel` for the spectral range of the cutout,
* `--output` for the directory where the cutouts, mosaics and temporary files will be stored, and
* `--config` for CASDA credentials (see below for template).

To run this program fist pull the required ASKAPsoft singularity image.

```
module load singularity/4.1.0-mpi
singularity pull askapsoft.sif docker://csirocass/askapsoft:1.15.0-setonix
```

Then create an `sbatch` script (called `run.sh` for example) with the arguments for the source of interest, output directory for your files, and pointing to the `askapsoft.sif` image:

```
#!/bin/bash

#SBATCH --account=ja3
#SBATCH --time=12:00:00
#SBATCH --mem=16G
module load python/3.11.6
module load py-pip/23.1.2-py3.11.6
pip install --user -r requirements.txt
python3 pipeline.py \
    --ra <RA [deg]> --dec <DEC [deg]> --radius <RADIUS [arcmin]> \
    --vel <VELOCITY RANGE [km/s]> \
    --config /path/to/casda.ini \
    --output /path/to/output_dir \
    --askapsoft /path/to/askapsoft.sif
```

**NOTE**: For Setonix the correct default module is provided in the snippet above, but otherwise run `module spider singularity` to find the correct version for your HPC environment

Then to run the program:

```
sbatch run.sh
```

## Configuration

To authenticate with [CASDA](https://data.csiro.au/) you will need a configuration file. A template `casda.ini`:

```
[CASDA]
username = <username>
password = <password>
```
