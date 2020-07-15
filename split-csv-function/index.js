const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');
const csvSplitStream = require('csv-split-stream');

const PROJECT_ID = 'store-big-file';
const SPLITTED_CSV_FILE_TOPIC_NAME = 'splitted-csv-file-topic';

/**
 * Generic background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.splitCsvFunction = (file, context, callback) => {
  if (!file.name.includes('input')) {
    return callback(null, null);
  }
  const storageClient = new Storage()
  const pubSubClient = new PubSub();
  const timestamp = Date.now();
  const fullTopicName = `projects/${PROJECT_ID}/topics/${SPLITTED_CSV_FILE_TOPIC_NAME}`;
  const outputFilenames = [];

  const readFileStream = storageClient.bucket(file.bucket).file(file.name).createReadStream();
  csvSplitStream.split(readFileStream, { lineLimit: 100 },
    (index) => {
      const outputFilename = `output-${timestamp}-${index}.csv`;
      outputFilenames.push(outputFilename);

      return storageClient.bucket(file.bucket).file(outputFilename).createWriteStream();
    }
  )
    .then(async () => {
      return await Promise.all(
        outputFilenames.map((filename) => {
          const payload = JSON.stringify({ data: { csvFilename: filename } });

          return pubSubClient
            .topic(fullTopicName)
            .publish(Buffer.from(payload));
        }),
      );
    })
    .then((publishedMessageIDs) => {
      callback(null, { success: true, publishedMessageIDs });
    })
    .catch((error) => {
      callback(error, { success: false });
    });
};
