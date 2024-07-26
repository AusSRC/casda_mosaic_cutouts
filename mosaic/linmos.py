#!/usr/bin/env python3

import os
import stat
import subprocess
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


def run_linmos(container, linmos_config, scratch, workdir, singularity, logger, **sbatch_kwargs):
    """Run linmos as a subprocess call with sbatch

    """
    if not os.path.exists(container):
        raise Exception(f'ASKAPsoft image not found at {container}')
    if not os.path.exists(linmos_config):
        raise Exception(f'Linmos config not found at {linmos_config}')

    sbatch_file = os.path.join(workdir, 'linmos.sh')
    cmd = [
        "#!/bin/bash\n",
        f"#SBATCH --account={sbatch_kwargs['account']}\n",
        f"#SBATCH --time={sbatch_kwargs['time']}\n",
        f"#SBATCH --mem={sbatch_kwargs['mem']}\n",
        f"module load {singularity}\n",
        f"singularity exec --bind {scratch}:{scratch} {container} linmos -c {linmos_config}\n"
    ]
    with open(sbatch_file, 'w') as f:
        f.writelines(cmd)
    st = os.stat(sbatch_file)
    os.chmod(sbatch_file, st.st_mode | stat.S_IEXEC)
    res = subprocess.run(f'sbatch {sbatch_file}', shell=True, check=True)
    return res


def generate_config(image_dict, weights_dict, output_image, output_weights, config, logger):
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
        images=[f.with_suffix('') for f in images],
        weights=[f.with_suffix('') for f in weights],
        image_out=image_out.with_suffix(''),
        weight_out=weight_out.with_suffix(''),
        image_history=image_history
    )

    # Write to linmos config
    with open(config, 'w') as f:
        print(result, file=f)

    return config
