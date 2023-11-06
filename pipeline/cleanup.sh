#!/bin/bash

printf "\n\n******Starting Cleanup*"
date

printf "\n\nDeleting SUCCEEDED batches a week ago\n"
WEEKAGO=`date -d "1 week ago" +"%Y-%m-%d"`
for batch in $(gcloud dataproc batches list --region=us-central1 --limit=1000 --filter="create_time<$WEEKAGO AND state=SUCCEEDED" --format='value(name)'); 
do 
  $(gcloud dataproc batches delete --quiet $batch --region=us-central1); 
done

# we want to retain failures for a month to give us time to investigate
printf "\n\nDeleting FAILED batches a month ago\n"
MONTHAGO=`date -d "1 month ago" +"%Y-%m-%d"`
for batch in $(gcloud dataproc batches list --region=us-central1 --limit=1000 --filter="create_time<$MONTHAGO AND state=FAILED" --format='value(name)'); 
do 
  $(gcloud dataproc batches delete --quiet $batch --region=us-central1); 
done

printf "\n\n******Finished Cleanup*"
