try:
    import boto
except ImportError:
    boto = None
import logging
import os
try:
    import sendgrid
except ImportError:
    sendgrid = None
import StringIO

try:
    from boto.s3.key import Key
except ImportError:
    Key = None

from csv import DictWriter


GEO_BUCKET = 'geo.tribapps.com'

if boto is not None:
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
            try:
                name = f.name.replace('%s/' % awaiting_folder, '')
                fkey = bucket.get_key(f.name)
                email_address = fkey.get_metadata('email')
                if name:
                    logging.info('Uploading %s to Bing' % name)
                    job_id = geocoder.upload_address_batch(fkey.get_contents_as_string())
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
                        old_key = Key(bucket)
                        old_key.key = '%s/%s' % (awaiting_folder, name)
                        old_key.delete()
                    else:
                        send_email_notification(email_address, {}, name, 'error')
            except Exception as e:
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

        connection = boto.connect_s3()
        bucket = connection.get_bucket(GEO_BUCKET)
        old_key = bucket.get_key('%s/%s' % (pending_folder, job_id))

        new_name = old_key.get_contents_as_string()
        new_key = Key(bucket)
        new_key.key = '%s/%s' % (finished_folder, new_name)

        results = geocoder.get_job_results(job_id)
        result_string = StringIO.StringIO()
        writer = DictWriter(result_string, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
        result_string.seek(0)

        email_address = old_key.get_metadata('email')
        if email_address:
            new_key.set_metadata('email', email_address)
            send_email_notification(
                email_address, geocoder.get_job_statuses(job_id=job_id), new_name, 'finished')

        new_key.set_contents_from_string(result_string.getvalue())
        new_key.make_public()
        old_key.delete()


if sendgrid:
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
