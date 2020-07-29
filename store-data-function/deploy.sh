APP_CREDENTIALS_FILE=~/.config/gcloud/application_default_credentials.json
PROJECT_ID=store-csv-file
ENTRY_POINT=storeDataFunction
TIMEOUT=540
FUNCTION_MEMORY=2048MB
TRIGGER_BUCKET_NAME=output-csv-storage-europe-west2
TRIGGER_TOPIC_NAME=output-csv-topic
REGION=europe-west2


if [ ! -f "$APP_CREDENTIALS_FILE" ]; then
  echo -e "Application default credentials ($APP_CREDENTIALS_FILE) don't exist, please finish the flow.\n"
  gcloud auth application-default login
fi

rm -rf dist
yarn build

gcloud config set functions/region $REGION
gcloud config set project $PROJECT_ID

gcloud functions deploy $ENTRY_POINT \
--runtime nodejs12 \
--memory=$FUNCTION_MEMORY \
--timeout=$TIMEOUT \
--trigger-topic=$TRIGGER_TOPIC_NAME \
--max-instances=1
--region=$REGION \
--project $PROJECT_ID
# --trigger-resource $TRIGGER_BUCKET_NAME \
# --trigger-event google.storage.object.finalize \
# --trigger-resource=$TRIGGER_TOPIC_NAME \
# --trigger-event=providers/cloud.pubsub/eventTypes/topic.publish \

