#!/usr/bin/env python3

import os
import sys
import math
import json
import keyring
import numpy as np
import asyncio
from configparser import ConfigParser
from argparse import ArgumentParser
from astropy.coordinates import SkyCoord
from astropy import units as u
from astroquery.casda import Casda
from astroquery.utils.tap.core import TapPlus
from prefect import task, get_run_logger


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
    parser.add_argument('--sbids', required=False, default=None, type=str, nargs='+', help='Specific SBIDs of the observations to filter to use for the cutouts')
    parser.add_argument('--milkyway', required=False, default=False, action='store_true', help='Filter for MilkyWay cubes (WALLABY specific query)')
    parser.add_argument('--no_keyring', required=False, default=False, action='store_true', help='Interactive prompt for password')
    parser.add_argument('--verbose', required=False, default=False, action='store_true', help='Verbose')
    args = parser.parse_args(argv)
    return args


def _cutout_and_download(casda, file_list, centre, radius, freq, output, logger, verbose):
    """Download and cutout with CASDA astroquery for a single cutout

    """
    filename = str(file_list[0]['filename'])
    logger.info(f'Cutout and download task for {filename} started')
    url_list = [f for f in casda.cutout(file_list, coordinates=centre, radius=radius*u.arcmin, band=freq, verbose=verbose) if '.checksum' not in f]
    res = casda.download_files(url_list, savedir=output)
    logger.info(f'Download complete: {filename}')
    download_filename = os.path.join(output, url_list[0].rsplit('/')[-1])
    filename_dict = {filename: download_filename}
    return filename_dict


async def _casda_download_subset(loop, casda, subset_sbid, centre, radius, freq, output, logger, verbose):
    """Download image and weight pairs for an observation async

    """
    img = subset_sbid[subset_sbid['dataproduct_subtype'] == 'spectral.restored.3d']
    wgt = subset_sbid[subset_sbid['dataproduct_subtype'] == 'spectral.weight.3d']
    img_task = loop.run_in_executor(None, _cutout_and_download, casda, img, centre, radius, freq, output, logger, verbose)
    wgt_task = loop.run_in_executor(None, _cutout_and_download, casda, wgt, centre, radius, freq, output, logger, verbose)
    img_dict, wgt_dict = await asyncio.gather(img_task, wgt_task)
    return img_dict, wgt_dict


@task
async def download(name, ra, dec, radius, freq, vel, obs_collection, output, config, url, sbids, query, milkyway, verbose, no_keyring, *args, **kwargs):
    loop = asyncio.get_running_loop()
    logger = get_run_logger()

    # Parse config
    assert os.path.exists(config), f'Config file not found at {config}'
    parser = ConfigParser()
    parser.read(config)

    if not no_keyring:
        logger.info('Keyring information:')
        logger.info(keyring.get_keyring())
        keyring.set_password(KEYRING_SERVICE, parser['CASDA']['username'], parser['CASDA']['password'])

    # Login to CASDA
    logger.info('Authenticating with CASDA')
    casda = Casda()
    if no_keyring:
        logger.info('No keyring found. CASDA will prompt for password.')
    casda.login(username=parser['CASDA']['username'])

    # Object coordinates
    if name is not None:
        centre = SkyCoord.from_name(name)
    elif (ra is not None) and (dec is not None):
        centre = SkyCoord(ra, dec, unit='deg')
    else:
        raise Exception('Either --name or --ra and --deg arguments are required.')
    logger.info(f'Centre coordinates: ({centre.ra}, {centre.dec})')

    freq = None
    if freq is not None:
        freq = np.array([float(i) for i in freq.split(' ')]) * 1e6 * u.Hz
    elif vel is not None:
        vel = np.array([float(i) for i in vel.split(' ')]) * u.km / u.s
        freq = vel.to(u.Hz, equivalencies=u.doppler_radio(HI_REST_FREQ))
        freq.sort()
    else:
        raise Exception('Either --freq [MHz] or --vel [km/s] range must be provided (space separated)')
    logger.info(f'Frequency range: {freq}')

    # TAP query for observations
    tap = TapPlus(url=url)
    query = query.replace('$OBS_COLLECTION', obs_collection)
    logger.info(f'Submitting query: {query}')
    job = tap.launch_job_async(query)
    observations = job.get_results()
    logger.info(observations)

    # Various filters
    logger.info('Filtered results')
    subset = observations[[('MilkyWay' in f) == milkyway for f in observations['filename']]]
    subset = subset[abs(subset['s_ra'] - centre.ra) < SEPARATION]
    subset = subset[abs(subset['s_dec'] - centre.dec) < SEPARATION]
    logger.info(subset)
    if len(subset) == 0:
        logger.info('No subset found based on search parameters.')
        return

    # Download with async
    if not os.path.exists(output):
        os.makedirs(output)

    task_list = []
    for obs_id in list(set(subset['obs_id'])):
        # Filter sbids
        if sbids is not None:
            if not any([sbid in obs_id for sbid in sbids]):
                logger.info(f'Filtering out {obs_id} observations')
                continue
        logger.info(f'Downloading cutouts for observation {obs_id}')
        subset_sbid = subset[subset['obs_id'] == obs_id]
        logger.info(subset_sbid)

        # Download image and weights files separately per sbid
        task = asyncio.create_task(
            _casda_download_subset(loop, casda, subset_sbid, centre, radius, freq, output, logger, verbose)
        )
        task_list.append(task)

    image_dict = {}
    weights_dict = {}
    task_res = await asyncio.gather(*task_list)
    for img_dict, wgt_dict in task_res:
        image_dict = {**image_dict, **img_dict}
        weights_dict = {**weights_dict, **wgt_dict}

    # Check file size
    total_size = 0
    all_files = list(image_dict.values()) + list(weights_dict.values())
    for f in all_files:
        total_size += os.path.getsize(f)
    logger.info(f'Downloaded {len(all_files)} files with total size {round(total_size / 1e6, 4)} MB')

    # Write file map
    logger.info(image_dict)
    logger.info(weights_dict)
    with open(os.path.join(output, 'file_map.json'), 'w') as f:
        json.dump({**image_dict, **weights_dict}, f)

    return (image_dict, weights_dict)


async def main(argv):
    args = parse_args(argv)
    image_dict, weights_dict = await download(**args.__dict__)


if __name__ == '__main__':
    argv = sys.argv[1:]
    asyncio.run(main(argv))
