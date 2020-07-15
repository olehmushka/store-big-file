const { Storage } = require('@google-cloud/storage');
const { Datastore } = require('@google-cloud/datastore');
const csvParser = require('csv-parser')

const CSV_BUCKET_NAME = 'csv-storage-1';
const SPLITTED_CSV_FILE_SUBSCRIPTION_NAME = 'splitted-csv-file-subscription';
const DATASTORE_COLLECTION_NAME = 'user-is-okay-list';

class CustomError extends Error {
  constructor({ message }) {
    super(message);
  }
}

class BadPubSubPayload extends CustomError {
  constructor({ message = 'Bad Pub/Sub Payload', subscriptionName, messageId } = {}) {
    super({ message });
    this.subscriptionName = subscriptionName;
    this.messageId = messageId;
  }
}

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.storeDataFunction = (pubSubMessage, context, callback) => {
  if (pubSubMessage.data == null) {
    return callback(new BadPubSubPayload({
      message: 'Data is empty',
      subscriptionName: SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }

  /*
  interface Payload {
    data: {
      csvFilename: string;
    };
  }
  */
  let csvFilename = '';
  try {
    const payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());
    csvFilename = payload.data.csvFilename || '';
  } catch {
    return callback(new BadPubSubPayload({
      message: 'Can not parse payload',
      subscriptionName: SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }

  if (csvFilename === '') {
    return callback(new BadPubSubPayload({
      message: 'CSV filename is empty',
      subscriptionName: SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }
  
  const storageClient = new Storage()
  const datastoreClient = new Datastore();
  const result = [];
  storageClient.bucket(CSV_BUCKET_NAME).file(csvFilename).createReadStream()
    .pipe(csvParser())
    .on('error', (error) => callback(error))
    .on('data', (data) => result.push(data))
    .on('end', async () => {
      try {
        await storageClient.bucket(CSV_BUCKET_NAME).file(csvFilename).delete();
        const key = datastoreClient.key(DATASTORE_COLLECTION_NAME);
        const transaction = datastoreClient.transaction();
        transaction.run(function(error) {
          if (error) {
            return callback(error);
          }

          result.forEach((item) => {
            transaction.save({ key, data: item });
          });

          transaction.commit(function(error) {
            if (!error) {
              return callback(error);
            }
            callback(null, { success: true });
          });
        });
      } catch (error) {
        return callback(error);
      }
    });
};
