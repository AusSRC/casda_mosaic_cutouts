#!/usr/bin/env python3

import os
import sys
import logging
from argparse import ArgumentParser
from cutout import casda
from mosaic import linmos
from prefect import task, flow


logging.basicConfig(level=logging.INFO)


def parse_args(argv):
    """CLI Arguments
    Either name or (ra, dec, radius) required
    Either freq or velocity required

    """
    parser = ArgumentParser()
    parser.add_argument('--name', type=str, required=False, default=None, help='Target source name')
    parser.add_argument('--ra', type=float, required=False, default=None, help='Centre RA [deg]')
    parser.add_argument('--dec', type=float, required=False, default=None, help='Centre Dec [deg]')
    parser.add_argument('--radius', type=float, required=True, default=None, help='Radius [arcmin]')
    parser.add_argument('--freq', type=str, required=False, default=None, help='Space-separated frequency range [MHz] (e.g. 1400 1440)')
    parser.add_argument('--vel', type=str, required=False, default=None, help='Space-separated velocity range [km/s]')
    parser.add_argument('--obs_collection', type=str, required=True, default=None, help='IVOA obscore "obs_collection" filter keyword')
    parser.add_argument('--output', type=str, required=True, default=None, help='Output directory for downloaded files')
    parser.add_argument('--config', type=str, required=True, help='CASDA credentials config file', default='casda.ini')
    parser.add_argument('--url', type=str, required=False, default=casda.CASDA_TAP_URL, help='TAP query URL')
    parser.add_argument('--query', type=str, required=False, default=casda.TAP_QUERY, help='IVOA obscore query string')
    parser.add_argument('--milkyway', required=False, default=False, action='store_true', help='Filter for MilkyWay cubes (WALLABY specific query)')
    parser.add_argument('--filename', type=str, required=False, default='mosaic.fits', help='Output filename for mosaicked image')
    parser.add_argument('--verbose', required=False, default=False, action='store_true', help='Verbose')
    parser.add_argument('--singularity', type=str, required=False, default='singularity/4.1.0', help='Singularity module')
    parser.add_argument('--scratch', type=str, required=False, default='/scratch', help='Scratch mount')
    parser.add_argument('--docker_image', type=str, required=False, default='docker://csirocass/askapsoft-1.15.0', help='ASKAPsoft docker image containing linmos')
    parser.add_argument('--container', type=str, required=False, default='askapsoft.sif', help='Singularity container name for ASKAPsoft image')
    parser.add_argument('--account', type=str, required=False, default='ja3', help='SBATCH header --account')
    parser.add_argument('--mem', type=str, required=False, default='32G', help='SBATCH header --mem')
    args = parser.parse_args(argv)
    return args


@task
def download_cutouts(**kwargs):
    image_dict, weight_dict = casda.download(**kwargs)
    return (image_dict, weight_dict)

@task
def generate_mosaic_config(image_dict, weight_dict, output_image, output_weights, output_config):
    linmos.generate_config(image_dict, weight_dict, output_image, output_weights, output_config)
    return output_config

@task
def mosaic(docker_image, container, linmos_config, **kwargs):
    image, weights = linmos.run_linmos(docker_image, container, linmos_config, **kwargs)
    return (image, weights)

@flow
def cutout_mosaic(argv):
    args = parse_args(argv)
    logging.info('Starting mosaic workflow')

    # Setup work environment
    workdir = args.output
    if not os.path.exists(workdir):
        logging.info(f'Output directory not found. Creating directory {workdir}')
        os.makedirs(workdir)
    output_image = os.path.join(workdir, args.filename)
    output_weights = os.path.join(workdir, f'weights.{args.filename}')
    linmos_config = os.path.join(workdir, 'linmos.conf')

    logging.info('Downloading cutouts')
    image_dict, weight_dict = download_cutouts(**args.__dict__)

    logging.info('Generating linmos config')
    linmos_config = generate_mosaic_config(image_dict, weight_dict, output_image, output_weights, linmos_config)

    # TODO: compare filesize with memory to ensure sufficient resources requested

    logging.info('Running linmos')
    container = os.path.join(workdir, args.container)
    mosaic(args.docker_image, container, linmos_config)
    if not os.path.exists(output_image) or not os.path.exists(output_weights):
        raise Exception('Pipeline error did not produce mosaicked images or weights')
    logging.info(f'Mosaic image file written to {output_image}')
    logging.info(f'Mosaic weights file written to {output_weights}')
    logging.info('Completed')
    return (output_image, output_weights)


if __name__ == '__main__':
    argv = sys.argv[1:]
    cutout_mosaic(argv)
