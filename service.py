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


def download_jobs(geocoder):
    """
    Download and submit jobs from S3.
    """
    logging.info('Downloading jobs')
    awaiting_folder = 'geocode_awaiting_submission'
    pending_folder = 'geocode_pending_jobs'
    connection = boto.connect_s3()
    bucket = connection.get_bucket(GEO_BUCKET)
    files = bucket.list('%s' % awaiting_folder)
    for f in files:
        name = f.name.replace('%s/' % awaiting_folder, '')
        if name:
            logging.info('Uploading %s to Bing' % name)
            job_id = geocoder.upload_address_batch(f.get_contents_as_string())
            if job_id:
                logging.info('Moving batch with old id %s to new id %s in %s' % (
                    name, job_id, pending_folder))
                new_key = Key(bucket)
                new_key.key = '%s/%s' % (pending_folder, job_id)
                new_key.set_contents_from_string(name)
                old_key = Key(bucket)
                old_key.key = '%s/%s' % (awaiting_folder, name)
                old_key.delete()


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
                k = Key(bucket)
                k.key = '%s/%s' % (pending_folder, job_id)
                settings = k.get_contents_as_string()
                save_job_results(geocoder, job_id)
                send_email_notification(results[0], settings, finished=True)


def save_job_results(geocoder, job_id):
    """
    Download and save to S3 results for completed jobs.
    """
    logging.info('Saving results for %s to S3' % job_id)
    finished_folder = 'geocode_finished_jobs'
    pending_folder = 'geocode_pending_jobs'
    connection = boto.connect_s3()
    bucket = connection.get_bucket(GEO_BUCKET)
    old_key = Key(bucket)
    old_key.key = '%s/%s' % (pending_folder, job_id)
    new_name = old_key.get_contents_as_string()
    new_key = Key(bucket)
    new_key.key = '%s/%s' % (finished_folder, new_name)
    results = geocoder.get_job_results(job_id)
    result_string = StringIO.StringIO()
    writer = DictWriter(result_string, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)
    result_string.seek(0)
    new_key.set_contents_from_string(result_string.getvalue())
    new_key.make_public()
    old_key.delete()


def send_email_notification(results, settings, finished=False):
    """
    Send an email notification when a job has been created or is finished.
    """
    finished_url = 'http://geo.tribapps.com/geocode_finished_jobs/%s' % settings
    if finished:
        subject = 'Finished geocoding job %s' % settings
        template = """
        Finished geocoding. Download results at {0}<br><br>

        Had trouble processing {1:,d} addresses out of {2:,d} submitted.
        """.format(finished_url, results['failedEntityCount'], results['processedEntityCount'])
    else:
        subject = 'Began processing geocode job %s' % settings
        template = """
        Just submitted job for geocoding.<br><br>

        We'll email you when it's done, but the results will be available at %s
        """
    sg = sendgrid.SendGridClient(
        os.environ.get('SENDGRID_USERNAME', ''), os.environ.get('SENDGRID_PASSWORD', ''))
    message = sendgrid.Mail(subject=subject, from_email='noreply@tribpub.com')
    message.add_to('aepton@tribpub.com')
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
