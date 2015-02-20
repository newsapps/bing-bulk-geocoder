#!/bin/sh

PROJECT=bing-bulk-geocoder

ROOT=/home/newsapps/sites/$PROJECT
SECRETS=/home/newsapps/sites/secrets/staging_secrets.sh
BING_SECRETS=/home/newsapps/sites/secrets/bing_secrets.sh
VIRTUALENV=/home/newsapps/.virtualenvs/$PROJECT/bin/activate

cd $ROOT
. $SECRETS
. $BING_SECRETS
. $VIRTUALENV

python service.py download
python service.py statuses
