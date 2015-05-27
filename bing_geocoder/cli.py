import logging
import logging.config
import os
from os.path import expanduser

import click

from .geocoder import (BingGeocoder, get_addresses_from_file,
    pretty_print_statuses, write_addresses_to_file)
try:
    from .service import download_jobs, check_pending_jobs
except ImportError:
    download_jobs = None
    check_pending_jobs = None

# Set up logging
logging_config = { 
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - line: %(lineno)d - %(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': '%s/logs/geocode_service.log' % expanduser('~'),
            'filename': '%s/logs/bing_geocoder.log' % expanduser('~'),
            'formatter': 'standard'
        }
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        }
    }
}

class ConfigurationError(Exception):
    pass

def get_geocoder(api_key=None):
    if api_key is None:
        try:
            env_var = 'BING_MAPS_API_KEY'
            api_key = os.environ[env_var]
        except KeyError:
            raise ConfigurationError("Need to provide Bing Maps API key")
            pass

    return BingGeocoder(api_key)

@click.group()
def cli():
    pass

@click.command()
@click.argument('path', type=click.Path(exists=True, dir_okay=False))
@click.option('--api-key', type=str, default=None, help="Bing Maps API key")
def upload(path, api_key=None):
    logging.config.dictConfig(logging_config)
    try:
        geocoder = get_geocoder(api_key)
    except ConfigurationError:    
        print('Error: Need to provide Bing Maps API key.')
        return

    batch = geocoder.batch_addresses(get_addresses_from_file(path))
    job_id = geocoder.upload_address_batch(batch)
    if job_id:
        print('Successful upload. Job id is {}'.format(job_id))

cli.add_command(upload)

@click.command()
@click.option('--api-key', type=str, default=None, help="Bing Maps API key")
def status(api_key=None):
    logging.config.dictConfig(logging_config)
    try:
        geocoder = get_geocoder(api_key)
    except ConfigurationError:    
        print('Error: Need to provide Bing Maps API key.')
        return

    pretty_print_statuses(geocoder.get_job_statuses())

cli.add_command(status)

@click.command()
@click.argument('job_id', type=str)
@click.argument('path', type=click.Path(dir_okay=False))
@click.option('--api-key', type=str, default=None, help="Bing Maps API key")
def download(job_id, path, api_key=None):
    logging.config.dictConfig(logging_config)
    try:
        geocoder = get_geocoder(api_key)
    except ConfigurationError:    
        print('Error: Need to provide Bing Maps API key.')
        return

    results = geocoder.get_job_results(job_id)
    if len(results):
        write_addresses_to_file(path, results)

cli.add_command(download)

@click.command()
@click.argument('task')
@click.option('--api-key', type=str, default=None, help="Bing Maps API key")
def service(task, api_key=None):
    if download_jobs is not None and check_pending_jobs is not None:
        # Check that these tasks are not None.  If they are None it probably
        # means boto isn't installed
        print("To run the service command, you need to install the boto package")
        return

    logfile = '{}/logs/geocode_service.log'.format(expanduser('~'))
    logging_config['handlers']['default']['filename'] = logfile
    logging.config.dictConfig(logging_config)
    try:
        geocoder = get_geocoder()
    except ConfigurationError as e:
        logging.error(e)
        return

    commands = {
        'download': download_jobs,
        'statuses': check_pending_jobs,
    }

    try:
        task_fn = commands[task]
    except KeyError:    
        logging.error('{} is an unsupported task'.format(task))
        return

    task_fn(geocoder)

cli.add_command(service)
