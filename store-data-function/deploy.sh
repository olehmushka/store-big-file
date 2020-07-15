APP_CREDENTIALS_FILE=~/.config/gcloud/application_default_credentials.json
PROJECT_ID=store-big-file
ENTRY_POINT=storeDataFunction
TOPIC_NAME=splitted-csv-file-topic
REGION=europe-west2

if [ ! -f "$APP_CREDENTIALS_FILE" ]; then
  echo -e "Application default credentials ($APP_CREDENTIALS_FILE) don't exist, please finish the flow.\n"
  gcloud auth application-default login
fi

yarn build

gcloud config set functions/region $REGION
gcloud config set project $PROJECT_ID

gcloud functions deploy $ENTRY_POINT \
--runtime nodejs10 \
--trigger-topic $TOPIC_NAME \
--region=$REGION \
--project $PROJECT_ID
