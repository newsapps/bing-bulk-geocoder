import json
import logging
import os
import pytz
import requests
import StringIO
import time

from csv import DictReader, DictWriter
from datetime import datetime, timedelta
from os.path import expanduser

# Set up logging
logging.config.dictConfig({
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
})


class BingGeocoder:
    """
    Class to handle uploading/checking on/downloading addresses for geocoding.
    """

    def __init__(self, key):
        if key:
            self.key = key
        else:
            self.key = os.env.get('BING_MAPS_API_KEY', None)

    def batch_addresses(self, addresses):
        """
        Given list of dicts {'address', 'entity_id'}, return a batch file containing their data.
        """
        if not addresses or not len(addresses):
            logging.error('Unable to upload blank list of addresses to geocode')
            return
        header_preamble = 'Bing Spatial Data Services, 2.0'
        header_fields = [
            'Id',
            'GeocodeRequest/Culture',
            'GeocodeRequest/ConfidenceFilter/MinimumConfidence',
            'GeocodeRequest/Query',
            'GeocodeResponse/Point/Latitude',
            'GeocodeResponse/Point/Longitude'
        ]
        header = '%s\n%s' % (header_preamble, ', '.join(header_fields))
        body = []
        line_format = '%s,en-US,High,"%s",,'
        for address in addresses:
            body.append(line_format % (address['entity_id'], address['address'].replace('"', '\"')))
        data = "%s\n%s" % (header, '\n'.join(body))
        logging.debug('Uploading %d addresses for geocoding' % len(body))
        return data

    def upload_address_batch(self, batch, prefix_preamble=True):
        """
        Given a string for a batch, send it to Bing for processing. If successful, returns string
        corresponding to job ID of uploaded batch.
        """
        url = ('http://spatial.virtualearth.net/REST/v1/Dataflows/Geocode?input=csv&key=%s' %
               self.key)
        if prefix_preamble:
            batch = 'Bing Spatial Data Services, 2.0\n%s' % batch
        try:
            r = requests.post(url, data=batch, headers={"Content-Type": "text/plain"})
            for rs in r.json()['resourceSets']:
                for resource in rs['resources']:
                    if 'id' in resource:
                        logging.debug('Successful upload; job id is %s' % resource['id'])
                        return resource['id']
            logging.warning('No job id found, job must not have been successfully uploaded')
        except Exception, e:
            logging.exception('Error uploading addresses: %s' % e)

    def get_job_statuses(self, min_cutoff=4320, only_completed=False, job_id=''):
        """
        Connect to Bing API and get list of all available jobs. Only return jobs completed after
        min_cutoff minutes ago; default is 72 hours' worth. Set to 0 to return all jobs. Set
        only_completed to True to see only completed jobs. If job_id is given, get_job_statuses
        will return a 1-element list containing the resource for that job.
        """
        url = ('http://spatial.virtualearth.net/REST/v1/dataflows/listjobs?key=%s' % self.key)
        now = datetime.now(pytz.UTC)
        delta = timedelta(minutes=min_cutoff)
        results = []
        r = requests.get(url)
        for rs in r.json()['resourceSets']:
            for resource in rs['resources']:
                if job_id:
                    if resource['id'] == job_id:
                        return [resource]
                if 'completedDate' not in resource and only_completed:
                    # Job hasn't completed and we only want completed jobs
                    continue
                elif min_cutoff != 0:
                    # We care about when the job was completed
                    if 'createdDate' in resource:
                        created = datetime.strptime(
                            resource['createdDate'],
                            '%a, %d %b %Y %H:%M:%S %Z')
                        if created.replace(tzinfo=pytz.UTC) > now - delta:
                            # Job was completed after min_cutoff minutes ago
                            results.append(resource)
                else:
                    results.append(resource)
        if job_id:
            # We were asked for specific job status, and since we're still here, we couldn't find it
            return []
        return results

    def get_job_results(self, job_id):
        """
        Connect to Bing API and get results of geocode jobs. If job_id is given, just return
        results for that job. Return list of (entity_id, address, lat, lng) tuples for each job
        id.
        """
        results = self.get_job_statuses(job_id=job_id)
        result_rows = []
        for result in results:
            if result['status'] == 'Completed':
                for link in result['links']:
                    if link.get('name', '') == 'succeeded':
                        url = '%s?key=%s' % (link['url'], self.key)
                        r = requests.get(url, headers={"Content-Type": "text/plain"})
                        if len(r.text.splitlines()) > 2:
                            result_data = StringIO.StringIO()
                            # Bing results data comes back with space+comma separator in header
                            result_data.write(
                                '%s\n' % r.text.splitlines()[1].replace(', ', ',').encode(
                                    'utf-8', errors='replace'))
                            for line in r.text.splitlines()[2:]:
                                result_data.write('%s\n' % line.encode('utf-8', errors='replace'))
                            result_data.flush()
                            result_data.seek(0)
                            # Iterating twice over file in order to rely on csv DictReader parsing
                            reader = DictReader(result_data)
                            for line in reader:
                                result_rows.append(line)
        return result_rows


def get_addresses_from_file(path):
    """
    Given a path to a file, open it and read it into a list of dicts. There should be no header row,
    and each line should be of the format: entity_id,address. Ideally, remove the commas from each
    address, but if you don't the script will try to deal.
    """
    addresses = []
    with open(path) as filehandle:
        for line in filehandle:
            data = line[:-1].split(',', 1)
            if len(data) == 2 and data[0] and data[1]:
                addresses.append({'entity_id': data[0], 'address': data[1]})
    if not len(addresses):
        print 'Heads up: not able to find any addresses in file %s' % path
    return addresses


def write_addresses_to_file(path, address_rows):
    """
    Given a path to a file, open it (creating it if it doesn't exist, overwriting if it does) and
    write a CSV to the file containing the addresses and their lat/longs.
    """
    fieldnames = [
        'Id',
        'GeocodeRequest/Query',
        'GeocodeResponse/Point/Latitude',
        'GeocodeResponse/Point/Longitude'
    ]
    with open(path, 'w+') as filehandle:
        writer = DictWriter(filehandle, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in address_rows:
            writer.writerow(row)


def pretty_print_statuses(statuses):
    """
    Convenience method to print job status info comprehensibly.
    """
    for status in statuses:
        print """
        Job ID: %s
        Created: %s
        Completed: %s
        Current status: %s
        Total entities: %s
        Processed entities: %s
        Failed entities: %s
        -----------------------
        """ % (
            status['id'],
            status['createdDate'],
            status.get('completedDate', '--'),
            status['status'],
            status['totalEntityCount'],
            status['processedEntityCount'],
            status['failedEntityCount']
        )


def main():
    env_var = 'BING_MAPS_API_KEY'
    if env_var in os.environ:
        geocoder = BingGeocoder(os.environ[env_var])
    else:
        key = raw_input('Enter Bing Maps API key: ')
        if key:
            geocoder = BingGeocoder(key)
        else:
            print 'Error: Need to enter Bing Maps API key.'
            return

    option = raw_input('Type g to (g)et results, u to (u)pload addresses for geocoding, s to (s)ee'
                       ' statuses of recent jobs: ')
    if option.lower() not in ['g', 'u', 's']:
        print 'Need to enter either g, s or u.'
        return

    if option.lower() == 'u':
        path = raw_input('Enter path to file containing id,address pairs: ')
        if not path:
            print 'Need to provide a path to a file.'
            return
        batch = geocoder.batch_addresses(get_addresses_from_file(path))
        job_id = geocoder.upload_address_batch(batch)
        if job_id:
            print 'Successful upload. Job id is %s' % job_id

    if option.lower() == 'g':
        job_id = raw_input('Enter job id to download results for, leave blank to see status of last'
                           ' 24 hours\' worth of jobs: ')
        results = geocoder.get_job_results(job_id)
        if len(results):
            path = raw_input('Enter path to file that will store geocoded addresses: ')
            if not path:
                print 'Need to provide a path to a file.'
                return
            write_addresses_to_file(path, results)

    if option.lower() == 's':
        pretty_print_statuses(geocoder.get_job_statuses())

if __name__ == '__main__':
    main()
