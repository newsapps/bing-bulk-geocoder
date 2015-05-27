#!/bin/sh

PROJECT=bing-bulk-geocoder

ROOT=/home/newsapps/sites/$PROJECT
SECRETS=/home/newsapps/sites/secrets/production_secrets.sh
BING_SECRETS=/home/newsapps/sites/secrets/bing_secrets.sh
SENDGRID_SECRETS=/home/newsapps/sites/secrets/sendgrid_secrets.sh
VIRTUALENV=/home/newsapps/.virtualenvs/$PROJECT/bin/activate

cd $ROOT
. $SECRETS
. $BING_SECRETS
. $SENDGRID_SECRETS
. $VIRTUALENV

bing_geocoder service download
bing_geocoder service statuses
