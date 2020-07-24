APP_CREDENTIALS_FILE=~/.config/gcloud/application_default_credentials.json
PROJECT_ID=store-csv-file
ENTRY_POINT=splitCsvFunction
TRIGGER_BUCKET_NAME=input-csv-storage-europe-west2
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
--runtime nodejs10 \
--trigger-resource $TRIGGER_BUCKET_NAME \
--trigger-event google.storage.object.finalize \
--region=$REGION \
--project $PROJECT_ID
