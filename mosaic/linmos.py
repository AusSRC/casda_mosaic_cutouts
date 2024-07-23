#!/usr/bin/env python3

import os
import subprocess
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


def pull_image(container, docker_image, singularity):
    subprocess.run(f'module load {singularity}', shell=True, check=True)
    subprocess.run(f'singularity pull {container} {docker_image}', shell=True, check=True)
    assert os.path.exists(container)
    return


def run_linmos(docker_image, container, linmos_config, singularity, *args, **kwargs):
    """Run linmos as a subprocess call

    """
    if not os.path.exists(container):
        pull_image(container, docker_image)
    if not os.path.exists(linmos_config):
        raise Exception(f'Linmos config not found at {linmos_config}')

    subprocess.run(f'module load {singularity}', shell=True, check=True)
    cmd = f"""#!/bin/bash
    # SBATCH --account={kwargs['account']}
    # SBATCH --mem={kwargs['mem']}
    module load {singularity}
    singularity exec --bind {kwargs['scratch']}:{kwargs['scratch']} linmos -c {linmos_config}
    """
    subprocess.run(cmd, shell=True, check=True)
    return


def generate_config(image_dict, weights_dict, output_image, output_weights, config):
    """Generate linmos config from template.
    Image and weights dicts are mappings from original fits cube to cutout filename. The original
    filenames will be stored in the image history.

    """
    images = [Path(image) for image in image_dict.values()]
    weights = [Path(weight) for weight in weights_dict.values()]
    image_out = Path(output_image)
    weight_out = Path(output_weights)
    image_history = list(image_dict.keys()) + list(weights_dict.keys())

    # Update config
    j2_env = Environment(loader=FileSystemLoader(f'{os.path.dirname(__file__)}/template'), trim_blocks=True)
    result = j2_env.get_template('linmos.j2').render(
        images=images,
        weights=weights,
        image_out=image_out,
        weight_out=weight_out,
        image_history=image_history
    )

    # Write to linmos config
    with open(config, 'w') as f:
        print(result, file=f)

    return config
