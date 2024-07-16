#!/usr/bin/env python3

import os
import sys
import logging
import asyncio
import keyring
from configparser import ConfigParser
from argparse import ArgumentParser
from astropy.coordinates import SkyCoord
from astropy import coordinates, units as u, wcs
from astroquery.casda import Casda


logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)


KEYRING_SERVICE = 'astroquery:casda.csiro.au'
URL = 'https://casda.csiro.au/casda_vo_tools/tap'
QUERY = 'SELECT * FROM iva.obscore '


def parse_args(argv):
    """CLI Arguments

    Either name or (ra, dec, radius) required
    Either freq or velocity required
    """

    parser = ArgumentParser()
    parser.add_argument('--name', type=str, required=False, help='Target source name')
    parser.add_argument('--ra', type=float, required=False, help='Centre RA [deg]')
    parser.add_argument('--dec', type=float, required=False, help='Centre Dec [deg]')
    parser.add_argument('--radius', type=float, required=False, help='Radius [arcmin]')
    parser.add_argument('--freq', type=float, required=False, help='Space-separated frequency range [MHz] (e.g. 1400 1440)')
    parser.add_argument('--velocity', type=float, required=False, help='Space-separated velocity range [km/s]')
    parser.add_argument('--url', type=str, required=False, default=URL, help='TAP query URL')
    parser.add_argument('--query', type=str, required=False, default=QUERY, help='IVOA obscore query string')
    parser.add_argument('--output', type=str, required=False, help='Output directory for downloaded files')
    parser.add_argument('--project', type=str, required=True, help='IVOA obscore "obs_collection" filter keyword')
    parser.add_argument('--config', type=str, required=True, help='CASDA credentials config file', default='casda.ini')
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
    logger.info('Authenticating with CASDA')
    casda = Casda()
    casda.login(username=config['CASDA']['username'])

    # Object
    if args.name is not None:
        centre = SkyCoord.from_name(args.name)
    elif (args.ra is not None) and (args.dec is not None):
        centre = SkyCoord(args.ra, args.dec, unit='deg')
    else:
        raise Exception('Either --name or --ra and --deg arguments are required.')
    logger.info(f'Centre coordinates: {centre}')

    # Query and filter
    result_table = casda.query_region(centre, radius=100*u.arcmin)
    logger.info(result_table)

    # Download
    pass

    return


if __name__ == '__main__':
    argv = sys.argv[1:]
    asyncio.run(main(argv))
