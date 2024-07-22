#!/usr/bin/env python3

import os
import sys
import math
import logging
import asyncio
import keyring
import numpy as np
from configparser import ConfigParser
from argparse import ArgumentParser
from astropy.coordinates import SkyCoord
from astropy import coordinates, units as u, wcs
from astroquery.casda import Casda
from astroquery.utils.tap.core import TapPlus


logging.basicConfig(level=logging.INFO)


KEYRING_SERVICE = 'astroquery:casda.csiro.au'
URL = 'https://casda.csiro.au/casda_vo_tools/tap'
QUERY = "SELECT * FROM ivoa.obscore WHERE (obs_collection LIKE '%$OBS_COLLECTION%' AND " \
        "quality_level != 'REJECTED' AND " \
        "(filename LIKE '%contsub%' OR filename LIKE '%weight%') AND" \
        "(dataproduct_subtype = 'spectral.restored.3d' OR dataproduct_subtype = 'spectral.weight.3d'))"
HI_REST_FREQ = 1.420405751786 * u.GHz
SEPARATION = math.sqrt(3**2 + 3**2)


def parse_args(argv):
    """CLI Arguments

    Either name or (ra, dec, radius) required
    Either freq or velocity required
    """

    parser = ArgumentParser()
    parser.add_argument('--name', type=str, required=False, help='Target source name')
    parser.add_argument('--ra', type=float, required=False, help='Centre RA [deg]')
    parser.add_argument('--dec', type=float, required=False, help='Centre Dec [deg]')
    parser.add_argument('--radius', type=float, required=True, help='Radius [arcmin]')
    parser.add_argument('--freq', type=str, required=False, help='Space-separated frequency range [MHz] (e.g. 1400 1440)')
    parser.add_argument('--vel', type=str, required=False, help='Space-separated velocity range [km/s]')
    parser.add_argument('--obs_collection', type=str, required=True, help='IVOA obscore "obs_collection" filter keyword')
    parser.add_argument('--output', type=str, required=True, help='Output directory for downloaded files')
    parser.add_argument('--config', type=str, required=True, help='CASDA credentials config file', default='casda.ini')
    parser.add_argument('--url', type=str, required=False, default=URL, help='TAP query URL')
    parser.add_argument('--query', type=str, required=False, default=QUERY, help='IVOA obscore query string')
    parser.add_argument('--milkyway', required=False, default=False, action='store_true', help='Filter for MilkyWay cubes (WALLABY specific query)')
    parser.add_argument('--verbose', required=False, default=False, action='store_true', help='Verbose')
    args = parser.parse_args(argv)
    return args


async def main(argv):
    args = parse_args(argv)

    # Parse config
    assert os.path.exists(args.config), f'Config file not found at {args.config}'
    config = ConfigParser()
    config.read(args.config)
    keyring.set_password(KEYRING_SERVICE, config['CASDA']['username'], config['CASDA']['password'])

    # Login to CASDA
    logging.info('Authenticating with CASDA')
    casda = Casda()
    casda.login(username=config['CASDA']['username'])

    # Object coordinates
    if args.name is not None:
        centre = SkyCoord.from_name(args.name)
    elif (args.ra is not None) and (args.dec is not None):
        centre = SkyCoord(args.ra, args.dec, unit='deg')
    else:
        raise Exception('Either --name or --ra and --deg arguments are required.')
    logging.info(f'Centre coordinates: ({centre.ra}, {centre.dec})')

    freq = None
    if args.freq is not None:
        freq = np.array([float(i) for i in args.freq.split(' ')]) * 1e6 * u.Hz
    elif args.vel is not None:
        vel = np.array([float(i) for i in args.vel.split(' ')]) * u.km / u.s
        freq = vel.to(u.Hz, equivalencies=u.doppler_radio(HI_REST_FREQ))
        freq.sort()
    else:
        raise Exception('Either --freq [MHz] or --vel [km/s] range must be provided (space separated)')
    logging.info(f'Frequency range: {freq}')

    # TAP query for observations
    tap = TapPlus(url=args.url)
    query = args.query.replace('$OBS_COLLECTION', args.obs_collection)
    logging.info(f'Submitting query: {query}')
    job = tap.launch_job_async(query)
    observations = job.get_results()
    logging.info(observations)

    # Various filters
    logging.info('Filtered results')
    subset = observations[[('MilkyWay' in f) == args.milkyway for f in observations['filename']]]
    subset = subset[abs(subset['s_ra'] - centre.ra) < SEPARATION]
    subset = subset[abs(subset['s_dec'] - centre.dec) < SEPARATION]
    logging.info(subset)
    if len(subset) == 0:
        logging.info('No subset found based on search parameters.')
        return

    # Create cutouts and download
    job_url = casda._create_job(subset, 'cutout_service', args.verbose)
    logging.info(f'Cutout url: {job_url}')
    cutout_spec = casda._args_to_payload(radius=args.radius*u.arcmin, coordinates=centre, band=freq, verbose=args.verbose)
    casda._add_cutout_params(job_url, args.verbose, cutout_spec)
    url_list = casda._complete_job(job_url, args.verbose)
    logging.info(url_list)

    # Create cutouts and download (image and weight files separately)
    if not os.path.exists(args.output):
        os.makedirs(args.output)
    file_list = casda.download_files(url_list, savedir=args.output)
    logging.info(file_list)

    # TODO: perform checksum check

    return


if __name__ == '__main__':
    argv = sys.argv[1:]
    asyncio.run(main(argv))
