APP_CREDENTIALS_FILE=~/.config/gcloud/application_default_credentials.json
PROJECT_ID=store-big-file
ENTRY_POINT=splitCsvFunction
TRIGGER_BUCKET_NAME=csv-storage-1
REGION=europe-west2

if [ ! -f "$APP_CREDENTIALS_FILE" ]; then
  echo -e "Application default credentials ($APP_CREDENTIALS_FILE) don't exist, please finish the flow.\n"
  gcloud auth application-default login
fi

gcloud config set functions/region $REGION
gcloud config set project $PROJECT_ID

gcloud functions deploy $ENTRY_POINT \
--runtime nodejs10 \
--trigger-resource $TRIGGER_BUCKET_NAME \
--trigger-event google.storage.object.finalize \
--region=$REGION \
--project $PROJECT_ID
