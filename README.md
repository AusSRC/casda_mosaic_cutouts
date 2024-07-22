# Cutout mosaic pipeline

A pipeline to generate mosaics of HI spectral cubes from the CASDA archive.

## Usage

Example

```
python3 cutout.py \
    --config /path/to/casda.ini --output /path/to/output_dir \
    --obs_collection WALLABY --ra 197.24113 --dec -15.51682 --radius 85.9434683 --vel '950 1550' \
```

## Configuration

To authenticate with [CASDA](https://data.csiro.au/) you will need a configuration file. A template `casda.ini`:

```
[CASDA]
username = <username>
password = <password>
```
