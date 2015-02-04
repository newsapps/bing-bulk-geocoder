# bing-bulk-geocoder
Python library to make bing bulk geocoding a wee bit easier.

## Warning

This is really early in development, so there may well be bugs or features you'd like to see. Please file issues for them so I know what to prioritize. Thanks.

## Setup

This script requires a [Bing Maps API key](http://www.microsoft.com/maps/create-a-bing-maps-key.aspx). It won't work at all without one. Once you get it, put it in your environment as BING_MAPS_API_KEY; if you use the script interactively and no key is present in your environment, it'll ask you for it. If you use the class, pass in the key as the only initialization parameter.

## Usage

This module can be used directly from the command line, or imported as a class in another script.

### Command-line usage

Put it in a virtualenv and install the requirements.txt file:

    pip install -r requirements.txt

Then, just run it:

    python bing_geocoder.py

It'll ask you if you want to upload addresses; get geocoded addresses back from Bing; or see status of recent jobs.

#### Uploading addresses

If you want to upload addresses, you'll need to give it the full path to a text file containing lines that look like this, with no header row:

    entity_id,address

entity_id is meant to make it easier for you to import the results into whatever system you need them for, without having to match strings with the address. Address should be either quoted or contain no commas, though if neither of those are true, it'll try to make it work anyway.

When the upload completes, the script will give you the job ID, which you'll need later to get the results back. If you lose track of it, you can still get your results later on; see the next section.

#### Seeing the status of recent jobs

When you select "see status", you'll be shown the status of jobs created within the last 72 hours, including their job ids. This will eventually be configurable.

#### Getting results for a job

When you select "get results", you'll be asked to enter the job id (determined above) to download results for, followed by the full path to save results to. It'll create a comma-separated file with a header row that will look like this:

    Id,GeocodeRequest/Query,GeocodeResponse/Point/Latitude,GeocodeResponse/Point/Longitude

Id is the entity id you assigned the row during upload. GeocodeRequest/Query is the original address you requested, and the other fields are what you thought they were ([and we let 'em off the hook!](http://www.youtube.com/watch?v=SWmQbk5h86w))

### Usage in your code

This module creates a class, BingGeocoder, with three methods (and an init):

    init(bing_maps_api_key):
        """
        That's right. When you initialize the class, you'll need to pass in your API key.
        """

    upload_addresses(addresses):
        """
        addresses is a list of dicts containing the keys "address" and "entity_id". See the above section on uploading addresses for info about entity_id.
        """

    get_job_statuses(min_cutoff=4320, only_completed=False, job_id=''):
        """
        min_cutoff is the number of minutes to go back in time to look for completed jobs. If specified and not 0, only jobs created after that many minutes ago will be returned.

        only_completed, if True, causes only jobs that have been completed to be returned in the list of job statuses.

        job_id, if specified, causes only status information for that job to be returned, as a single-element list.
        """

    get_job_results(job_id):
        """
        job_id is required. This will return a list of dicts containing the result data for your job; see the section on getting results for a job for an incredibly brief and incomplete discussion of what the returned fields are.
        """
