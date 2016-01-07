import argparse
import boto
import json
import logging
import logging.config
import os
import sendgrid
import StringIO

from boto.s3.key import Key
from bing_geocoder import BingGeocoder
from csv import DictReader, DictWriter
from datetime import date, datetime, timedelta
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
            'filename': '%s/logs/geocode_service.log' % expanduser('~'),
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

GEO_BUCKET = 'geo.tribapps.com'


def convert_dict_to_string(data):
    result_string = StringIO.StringIO()
    writer = DictWriter(result_string, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    result_string.seek(0)
    return result_string.getvalue()


def merge_data(data, extra_data):
    """
    Given a row of dicts (data) and a string containing a CSV of data (extra_data), return row of
    dicts merging the two together.
    """
    extra_rows = {}
    extra_file = StringIO.StringIO(extra_data)
    reader = DictReader(extra_file)
    for row in reader:
        extra_rows[row['Id']] = row
    for row in data:
        if row['Id'] in extra_rows:
            row.update(extra_rows[row['Id']])
    return data


def separate_bing_acceptable_data(data_str):
    """
    Given a string containing a CSV of data, return two strings containing CSVs of data:
    1 containing Id plus all Bing fields; 1 containing Id plus all non-Bing fields
    """
    bing_data = []
    extra_data = []
    bing_fields = [
        'GeocodeRequest/Culture',
        'GeocodeRequest/ConfidenceFilter/MinimumConfidence',
        'ReverseGeocodeRequest/Location/Latitude',
        'ReverseGeocodeRequest/Location/Longitude',
        'GeocodeResponse/Address/FormattedAddress',
        'GeocodeResponse/Point/Latitude',
        'GeocodeResponse/Point/Longitude',
        'GeocodeRequest/Query'
    ]
    data_file = StringIO.StringIO(data_str)
    reader = DictReader(data_file)
    for row in reader:
        bing_row = {}
        extra_row = {}
        for col in row.keys():
            if col.lower() == 'id':
                bing_row['Id'] = row['Id']
                extra_row['Id'] = row['Id']
            elif col in bing_fields:
                bing_row[col] = row[col]
            else:
                extra_row[col] = row[col]
        bing_data.append(bing_row)
        if len(extra_row.keys()) > 1:
            extra_data.append(extra_row)
    return (convert_dict_to_string(bing_data), convert_dict_to_string(extra_data))


def download_jobs(geocoder):
    """
    Download and submit jobs from S3.
    """
    logging.info('Downloading jobs')
    awaiting_folder = 'geocode_awaiting_submission'
    pending_folder = 'geocode_pending_jobs'
    extra_folder = 'geocode_extra_fields'
    connection = boto.connect_s3()
    bucket = connection.get_bucket(GEO_BUCKET)
    files = bucket.list('%s' % awaiting_folder)
    for f in files:
        try:
            name = f.name.replace('%s/' % awaiting_folder, '')
            fkey = bucket.get_key(f.name)
            email_address = fkey.get_metadata('email')
            if name:
                logging.info('Uploading %s to Bing' % name)
                old_key = Key(bucket)
                old_key.key = '%s/%s' % (awaiting_folder, name)
                # Need to strip out any non-Bing-acceptable fields
                # 1. Given CSV, create 2 rows of dicts, each w/field for Id: 1 for Bing, 1 to stash
                # 2. Put stashed CSV/dict in new folder, geocode_stashed, with orig filename
                # 3. In save_job_results, if filename match in geocode_stashed:
                #   a) Download stashed file
                #   b) Merge stashed file with results from Bing, keyed on Id
                #   c) Delete stashed file and put merged file in geocode_finished_jobs
                bing_data, extra_data = separate_bing_acceptable_data(fkey.get_contents_as_string())
                job_id = geocoder.upload_address_batch(bing_data, prefix_preamble=True)
                if job_id:
                    logging.info('Moving batch with old id %s to new id %s in %s' % (
                        name, job_id, pending_folder))
                    new_key = Key(bucket)
                    new_key.key = '%s/%s' % (pending_folder, job_id)
                    if email_address:
                        logging.info('Setting metadata to %s' % email_address)
                        new_key.set_metadata('email', email_address)
                        send_email_notification(email_address, {}, name, 'pending')
                    new_key.set_contents_from_string(name)
                    extra_key = Key(bucket)
                    extra_key.key = '%s/%s' % (extra_folder, name)
                    extra_key.set_contents_from_string(extra_data)
                    old_key.delete()
                else:
                    send_email_notification(email_address, {}, name, 'error')
                    old_key.delete()
        except Exception, e:
            logging.warning('Error uploading %s to Bing: %s' % (name, e))


def check_pending_jobs(geocoder):
    """
    Get list of pending jobs, look up their statuses and save the completed ones.
    """
    logging.info('Checking pending job')
    pending_folder = 'geocode_pending_jobs'
    connection = boto.connect_s3()
    bucket = connection.get_bucket(GEO_BUCKET)
    files = bucket.list('%s' % pending_folder)
    for f in files:
        job_id = f.name.replace('%s/' % pending_folder, '')
        if job_id:
            results = geocoder.get_job_statuses(job_id=job_id)
            if results[0]['status'] == 'Completed':
                k = bucket.get_key('%s/%s' % (pending_folder, job_id))
                settings = k.get_contents_as_string()
                save_job_results(geocoder, job_id)


def save_job_results(geocoder, job_id):
    """
    Download and save to S3 results for completed jobs.
    """
    logging.info('Saving results for %s to S3' % job_id)
    finished_folder = 'geocode_finished_jobs'
    pending_folder = 'geocode_pending_jobs'
    extra_folder = 'geocode_extra_fields'

    connection = boto.connect_s3()
    bucket = connection.get_bucket(GEO_BUCKET)
    old_key = bucket.get_key('%s/%s' % (pending_folder, job_id))

    new_name = old_key.get_contents_as_string()
    new_key = Key(bucket)
    new_key.key = '%s/%s' % (finished_folder, new_name)

    results = geocoder.get_job_results(job_id)

    extra_key = bucket.get_key('%s/%s' % (extra_folder, new_name))
    extra_data = extra_key.get_contents_as_string()
    result_string = convert_dict_to_string(merge_data(results, extra_data))
    extra_key.delete()

    email_address = old_key.get_metadata('email')
    if email_address:
        new_key.set_metadata('email', email_address)
        send_email_notification(
            email_address, geocoder.get_job_statuses(job_id=job_id), new_name, 'finished')

    new_key.set_contents_from_string(result_string.getvalue())
    new_key.make_public()
    old_key.delete()


def send_email_notification(address, results, settings, status):
    """
    Send an email notification when a job has been created or is finished.
    """
    finished_url = 'http://geo.tribapps.com/geocode_finished_jobs/%s' % settings
    if status == 'finished':
        if not results:
            results = [{}]
        subject = 'Finished geocoding job %s' % settings
        template = """
        Finished geocoding. Download results at {0}<br><br>

        Had trouble processing {1:,d} addresses out of {2:,d} submitted.
        """.format(
            finished_url,
            results[0].get('failedEntityCount', 0),
            results[0].get('processedEntityCount', 0))
    elif status == 'pending':
        subject = 'Began processing geocode job %s' % settings
        template = """
        Just submitted job for geocoding.<br><br>

        We'll email you when it's done, but the results will be available at %s
        """ % finished_url
    elif status == 'error':
        subject = 'Error processing geocode job %s' % settings
        template = """
        It looks like Bing had trouble processing the file you uploaded.<br><br>

        Was it a CSV with column headings as described at http://geo.tribapps.com?
        """
    sg = sendgrid.SendGridClient(
        os.environ.get('SENDGRID_USERNAME', ''), os.environ.get('SENDGRID_PASSWORD', ''))
    message = sendgrid.Mail(subject=subject, from_email='noreply@tribpub.com')
    message.add_to(address)
    message.set_html(template)
    message.set_text(template)
    logging.info('Sending email report to %s' % ', '.join(message.to))
    status, msg = sg.send(message)
    if status != 200:
        logging.warning('Error sending email, got status %s: %s' % (status, msg))


def main():
    env_var = 'BING_MAPS_API_KEY'
    if env_var in os.environ:
        geocoder = BingGeocoder(os.environ[env_var])
    else:
        logging.error('Error: Need to provide Bing Maps API key')
        return

    parser = argparse.ArgumentParser()
    parser.add_argument('task')
    args = parser.parse_args()
    commands = ['download', 'statuses']
    if args.task in commands:
        logging.info('Running %s' % args.task)
        if args.task == 'download':
            download_jobs(geocoder)
        elif args.task == 'statuses':
            check_pending_jobs(geocoder)
    else:
        logging.error('%s is an unsupported task' % args.task)
        return


if __name__ == '__main__':
    main()
