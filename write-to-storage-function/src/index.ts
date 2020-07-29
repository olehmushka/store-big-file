import { Firestore } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';
import { PubSub } from '@google-cloud/pubsub';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { FirestoreClient } from './libs/firestore-client';
import { PubSubClient } from './libs/pubsub-client';
import { StorageClient } from './libs/storage-client';
import { WriteToStorage } from './hanlder';
import config from './config';

type BaseFunctionInputData = {};

interface ISplitCSVDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const historyBucketName = config.HISTORY_CSV_BACKET;
const outputBucketName = config.OUTPUT_CSV_BACKET;

const logger = pino({ level: 'info' });
const firestore = new Firestore();
const storage = new Storage();
const pubsub = new PubSub();

const historyStorageClient = new StorageClient(storage, { bucketName: historyBucketName });
const outputStorageClient = new StorageClient(storage, { bucketName: outputBucketName });
const statisticFirestoreClient = new FirestoreClient(firestore, {
  collectionName: config.STATISTIC_DATASTORE_COLLECTION_NAME,
});
const pubsubClient = new PubSubClient(pubsub, { topicName: config.OUTPUT_FILE_TOPIC });
const handler = new WriteToStorage(
  logger,
  historyStorageClient,
  outputStorageClient,
  statisticFirestoreClient,
  pubsubClient,
);

export const writeToStorageFunction = async (
  file: ISplitCSVDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  const startDate = new Date();
  const details = {
    filename: file.name,
    inputBucketName: historyBucketName,
    outputBucketName: outputBucketName,
  };

  logger.info('Started Function', { filename: file.name, bucketName: historyBucketName });

  handler
    .handle(file.name)
    .then((result) => {
      const endDate   = new Date();
      const duration = (endDate.getTime() - startDate.getTime()) / 1000;

      if (result.isLeft()) {
        logger.error(`Function is failed in ${duration}`, details, result.value);

        return callback(result.value, { success: false });
      }

      logger.info(`Function is succeed in ${duration} for ${result.value} files`, details);

      return callback(null, { success: true });
    })
    .catch((error) => {
      logger.error('Function Error:', details, error);
      callback(error, { success: false });
    });
};
