#!/usr/bin/env python3

import os
import sys
import math
import logging
import keyring
import numpy as np
from configparser import ConfigParser
from argparse import ArgumentParser
from astropy.coordinates import SkyCoord
from astropy import units as u
from astroquery.casda import Casda
from astroquery.utils.tap.core import TapPlus


logging.basicConfig(level=logging.INFO)


KEYRING_SERVICE = 'astroquery:casda.csiro.au'
CASDA_TAP_URL = 'https://casda.csiro.au/casda_vo_tools/tap'
TAP_QUERY = "SELECT * FROM ivoa.obscore WHERE (obs_collection LIKE '%$OBS_COLLECTION%' AND " \
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
    parser.add_argument('--name', type=str, required=False, default=None, help='Target source name')
    parser.add_argument('--ra', type=float, required=False, default=None, help='Centre RA [deg]')
    parser.add_argument('--dec', type=float, required=False, default=None, help='Centre Dec [deg]')
    parser.add_argument('--radius', type=float, required=True, default=None, help='Radius [arcmin]')
    parser.add_argument('--freq', type=str, required=False, default=None, help='Space-separated frequency range [MHz] (e.g. 1400 1440)')
    parser.add_argument('--vel', type=str, required=False, default=None, help='Space-separated velocity range [km/s]')
    parser.add_argument('--obs_collection', type=str, required=False, default='WALLABY', help='IVOA obscore "obs_collection" filter keyword')
    parser.add_argument('--output', type=str, required=True, default=None, help='Output directory for downloaded files')
    parser.add_argument('--config', type=str, required=True, help='CASDA credentials config file', default='casda.ini')
    parser.add_argument('--url', type=str, required=False, default=CASDA_TAP_URL, help='TAP query CASDA_TAP_URL')
    parser.add_argument('--query', type=str, required=False, default=TAP_QUERY, help='IVOA obscore query string')
    parser.add_argument('--milkyway', required=False, default=False, action='store_true', help='Filter for MilkyWay cubes (WALLABY specific query)')
    parser.add_argument('--verbose', required=False, default=False, action='store_true', help='Verbose')
    args = parser.parse_args(argv)
    return args


def download(name, ra, dec, radius, freq, vel, obs_collection, output, config, url, query, milkyway, verbose, *args, **kwargs):
    # Parse config
    assert os.path.exists(config), f'Config file not found at {config}'
    parser = ConfigParser()
    parser.read(config)
    keyring.set_password(KEYRING_SERVICE, parser['CASDA']['username'], parser['CASDA']['password'])

    # Login to CASDA
    logging.info('Authenticating with CASDA')
    casda = Casda()
    casda.login(username=parser['CASDA']['username'])

    # Object coordinates
    if name is not None:
        centre = SkyCoord.from_name(name)
    elif (ra is not None) and (dec is not None):
        centre = SkyCoord(ra, dec, unit='deg')
    else:
        raise Exception('Either --name or --ra and --deg arguments are required.')
    logging.info(f'Centre coordinates: ({centre.ra}, {centre.dec})')

    freq = None
    if freq is not None:
        freq = np.array([float(i) for i in freq.split(' ')]) * 1e6 * u.Hz
    elif vel is not None:
        vel = np.array([float(i) for i in vel.split(' ')]) * u.km / u.s
        freq = vel.to(u.Hz, equivalencies=u.doppler_radio(HI_REST_FREQ))
        freq.sort()
    else:
        raise Exception('Either --freq [MHz] or --vel [km/s] range must be provided (space separated)')
    logging.info(f'Frequency range: {freq}')

    # TAP query for observations
    tap = TapPlus(url=url)
    query = query.replace('$OBS_COLLECTION', obs_collection)
    logging.info(f'Submitting query: {query}')
    job = tap.launch_job_async(query)
    observations = job.get_results()
    logging.info(observations)

    # Various filters
    logging.info('Filtered results')
    subset = observations[[('MilkyWay' in f) == milkyway for f in observations['filename']]]
    subset = subset[abs(subset['s_ra'] - centre.ra) < SEPARATION]
    subset = subset[abs(subset['s_dec'] - centre.dec) < SEPARATION]
    logging.info(subset)
    if len(subset) == 0:
        logging.info('No subset found based on search parameters.')
        return

    # Create cutouts, separate images and weights, download
    images = subset[subset['dataproduct_subtype'] == 'spectral.restored.3d']
    weights = subset[subset['dataproduct_subtype'] == 'spectral.weight.3d']
    images.sort('filename')
    weights.sort('filename')
    image_url_list = casda.cutout(images, coordinates=centre, radius=radius*u.arcmin, band=freq, verbose=verbose)
    weights_url_list = casda.cutout(weights, coordinates=centre, radius=radius*u.arcmin, band=freq, verbose=verbose)
    logging.info(f'Cutout image files: {image_url_list}')
    logging.info(f'Cutout weight files: {weights_url_list}')

    # Create cutouts and download
    if not os.path.exists(output):
        os.makedirs(output)
    images_list = casda.download_files(image_url_list, savedir=output)
    weights_list = casda.download_files(weights_url_list, savedir=output)
    logging.info(images_list)
    logging.info(weights_list)

    # Match original file and downloaded cutout for return
    image_url_list_nochecksum = [f for f in image_url_list if '.checksum' not in f]
    weights_url_list_nochecksum = [f for f in weights_url_list if '.checksum' not in f]
    assert len(image_url_list_nochecksum) == len(images), f"Number of image files {len(images)} and downloaded cutout files {len(image_url_list_nochecksum)} are not equal."
    assert len(weights_url_list_nochecksum) == len(weights), f"Number of weight files {len(weights)} and downloaded cutout files {len(weights_url_list_nochecksum)} are not equal."
    image_cutouts = [os.path.join(output, f.rsplit('/')[-1]) for f in image_url_list_nochecksum]
    weight_cutouts = [os.path.join(output, f.rsplit('/')[-1]) for f in weights_url_list_nochecksum]
    image_dict = dict(zip(list(images['filename']), image_cutouts))
    weights_dict = dict(zip(list(weights['filename']), weight_cutouts))
    logging.info(image_dict)
    logging.info(weights_dict)

    # Check file size
    total_size = 0
    for f in image_cutouts + weight_cutouts:
        total_size += os.path.getsize(f)
    logging.info(f'Mosaicking {len(image_cutouts)} files with total size {round(total_size / 1e6, 4)} MB')

    # TODO: perform checksum check
    return (image_dict, weights_dict)


if __name__ == '__main__':
    argv = sys.argv[1:]
    args = parse_args(argv)
    download(**args.__dict__)
