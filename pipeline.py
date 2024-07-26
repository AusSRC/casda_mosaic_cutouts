#!/usr/bin/env python3

import os
import sys
import logging
from argparse import ArgumentParser
from prefect import flow, get_run_logger
from cutout import casda
from mosaic import linmos


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
    parser.add_argument('--sbids', required=False, default=None, type=str, nargs='+', help='Specific SBIDs of the observations to filter to use for the cutouts')
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
    parser.add_argument('--askapsoft_docker', type=str, required=False, default='csirocass/askapsoft:1.15.0-setonix', help='Docker image for ASKAPsoft container')
    parser.add_argument('--local', required=False, default=False, action='store_true', help='Run locally')
    args = parser.parse_args(argv)
    return args


@flow
def cutout_mosaic(argv):
    logger = get_run_logger()
    args = parse_args(argv)
    logger.info('Starting mosaic workflow')

    # Setup work environment
    workdir = args.output
    if not os.path.exists(workdir):
        logger.info(f'Output directory not found. Creating directory {workdir}')
        os.makedirs(workdir)

    # Download images and weights
    logger.info('Downloading cutouts')
    image_dict, weight_dict = casda.download(**args.__dict__)

    # Generate linmos config
    linmos_config = os.path.join(workdir, 'linmos.conf')
    output_image = os.path.join(workdir, args.filename)
    output_weights = os.path.join(workdir, f'weights_{args.filename}')
    logger.info(f'Generating linmos config {linmos_config}')
    linmos.generate_config(image_dict, weight_dict, output_image, output_weights, linmos_config)

    # TODO: compare filesize with memory to ensure sufficient resources requested
    if args.local:
        linmos.run_linmos_docker(args.askapsoft_docker, workdir, linmos_config)
    else:
        sbatch_kwargs = {
            'account': args.account,
            'time': args.time,
            'mem': args.mem
        }
        linmos.run_linmos(args.askapsoft, linmos_config, args.scratch, workdir, args.singularity, **sbatch_kwargs)
    return (output_image, output_weights)


if __name__ == '__main__':
    argv = sys.argv[1:]
    cutout_mosaic(argv)
