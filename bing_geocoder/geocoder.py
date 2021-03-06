import logging
import os
import pytz
import requests
from six import StringIO

import csv
from datetime import datetime, timedelta


class BingGeocoder:
    """
    Class to handle uploading/checking on/downloading addresses for geocoding.
    """

    def __init__(self, key):
        if key:
            self.key = key
        else:
            self.key = os.env.get('BING_MAPS_API_KEY', None)

    def batch_addresses(self, addresses, prefix_preamble=False):
        """
        Given list of dicts {'address', 'entity_id'}, return a batch file containing their data.
        """
        if not addresses or not len(addresses):
            logging.error('Unable to upload blank list of addresses to geocode')
            return
        out = StringIO()
        writer = csv.writer(out)
        header_preamble = 'Bing Spatial Data Services, 2.0'
        if prefix_preamble:
            writer.writerow([header_preamble])

        header_fields = [
            'Id',
            'GeocodeRequest/Culture',
            'GeocodeRequest/ConfidenceFilter/MinimumConfidence',
            'GeocodeRequest/Query',
            'GeocodeResponse/Point/Latitude',
            'GeocodeResponse/Point/Longitude'
        ]
        writer.writerow(header_fields)

        for address in addresses:
            row = [address['entity_id'], "en-US", "High", address['address']]
            writer.writerow(row)

        logging.debug("Uploading {} addresses for geocoding".format(len(addresses)))
        payload = out.getvalue()

        return payload

    def upload_address_batch(self, batch, prefix_preamble=True):
        """
        Given a string for a batch, send it to Bing for processing. If successful, returns string
        corresponding to job ID of uploaded batch.
        """
        url = ("http://spatial.virtualearth.net/REST/v1/Dataflows/Geocode?input=csv&key=" +
               self.key)
        if prefix_preamble:
            batch = "Bing Spatial Data Services, 2.0\n{}".format(batch)
        try:
            r = requests.post(url, data=batch, headers={"Content-Type": "text/plain"})
            for rs in r.json()['resourceSets']:
                for resource in rs['resources']:
                    if 'id' in resource:
                        logging.debug("Successful upload; job id is " + resource['id'])
                        return resource['id']

            logging.warning("No job id found, job must not have been successfully uploaded")

        except Exception as e:
            logging.exception("Error uploading addresses: {}".format(e))

    def upload_addresses(self, addresses, prefix_preamble=True):
        batch = self.batch_addresses(addresses)
        return self.upload_address_batch(batch, prefix_preamble=prefix_preamble)

    def get_job_statuses(self, min_cutoff=4320, only_completed=False, job_id=''):
        """
        Connect to Bing API and get list of all available jobs. Only return jobs completed after
        min_cutoff minutes ago; default is 72 hours' worth. Set to 0 to return all jobs. Set
        only_completed to True to see only completed jobs. If job_id is given, get_job_statuses
        will return a 1-element list containing the resource for that job.
        """
        url = ("http://spatial.virtualearth.net/REST/v1/dataflows/listjobs?key="
               + self.key)
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
                        url = '{}?key={}'.format(link['url'], self.key)
                        r = requests.get(url, headers={"Content-Type": "text/plain"})

                        if len(r.text.splitlines()) > 2:
                            result_data = StringIO()
                            # Bing results data comes back with space+comma separator in header
                            result_data.write("{}\n".format(
                                r.text.splitlines()[1].replace(', ', ',')
                            ))

                            for line in r.text.splitlines()[2:]:
                                result_data.write("{}\n".format(
                                    line))

                            result_data.flush()
                            result_data.seek(0)
                            # Iterating twice over file in order to rely on csv DictReader parsing
                            reader = csv.DictReader(result_data)
                            for row in reader:
                                result_rows.append(row)

        return result_rows


def get_addresses_from_file(path):
    """
    Given a path to a file, open it and read it into a list of dicts. There should be no header row,
    and each line should be of the format: entity_id,address. Ideally, remove the commas from each
    address, but if you don't the script will try to deal.
    """
    addresses = []
    with open(path) as f:
        reader = csv.reader(f)
        for row in reader:
            addresses.append({'entity_id': row[0], 'address': row[1]})

    if not len(addresses):
        print('Heads up: not able to find any addresses in file {}'.format(path))

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
        writer = csv.DictWriter(filehandle, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in address_rows:
            writer.writerow(row)


def pretty_print_statuses(statuses):
    """
    Convenience method to print job status info comprehensibly.
    """
    for status in statuses:
        msg = """
        Job ID: {}
        Created: {}
        Completed: {}
        Current status: {}
        Total entities: {}
        Processed entities: {}
        Failed entities: {}
        -----------------------
        """.format(
            status['id'],
            status['createdDate'],
            status.get('completedDate', '--'),
            status['status'],
            status['totalEntityCount'],
            status['processedEntityCount'],
            status['failedEntityCount']
        )
        print(msg)
