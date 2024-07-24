#!/usr/bin/env python3

import os
import sys
import logging
from argparse import ArgumentParser
from prefect import task, flow
from cutout import casda
from mosaic import linmos


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
    parser.add_argument('--obs_collection', type=str, required=False, default='WALLABY', help='IVOA obscore "obs_collection" filter keyword')
    parser.add_argument('--output', type=str, required=True, default=None, help='Output directory for downloaded files')
    parser.add_argument('--config', type=str, required=True, help='CASDA credentials config file', default='casda.ini')
    parser.add_argument('--url', type=str, required=False, default=casda.CASDA_TAP_URL, help='TAP query URL')
    parser.add_argument('--query', type=str, required=False, default=casda.TAP_QUERY, help='IVOA obscore query string')
    parser.add_argument('--milkyway', required=False, default=False, action='store_true', help='Filter for MilkyWay cubes (WALLABY specific query)')
    parser.add_argument('--filename', type=str, required=False, default='mosaic.fits', help='Output filename for mosaicked image')
    parser.add_argument('--verbose', required=False, default=False, action='store_true', help='Verbose')
    parser.add_argument('--singularity', type=str, required=False, default='singularity/4.1.0-mpi', help='Singularity module')
    parser.add_argument('--scratch', type=str, required=False, default='/scratch', help='Scratch mount')
    parser.add_argument('--askapsoft', type=str, required=False, default='askapsoft.sif', help='Path to ASKAPsoft singularity container')
    parser.add_argument('--account', type=str, required=False, default='ja3', help='SBATCH header --account')
    parser.add_argument('--time', type=str, required=False, default='1:00:00', help='SBATCH header --time')
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
def mosaic(container, linmos_config, scratch, workdir, singularity, **sbatch_kwargs):
    res = linmos.run_linmos(container, linmos_config, scratch, workdir, singularity, **sbatch_kwargs)
    return res

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
    sbatch_kwargs = {
        'account': args.account,
        'time': args.time,
        'mem': args.mem
    }
    mosaic(args.askapsoft, linmos_config, args.scratch, workdir, args.singularity, **sbatch_kwargs)
    if not os.path.exists(output_image) or not os.path.exists(output_weights):
        raise Exception('Pipeline error did not produce mosaicked images or weights')
    logging.info(f'Mosaic image file written to {output_image}')
    logging.info(f'Mosaic weights file written to {output_weights}')
    logging.info('Completed')
    return (output_image, output_weights)


if __name__ == '__main__':
    argv = sys.argv[1:]
    cutout_mosaic(argv)
