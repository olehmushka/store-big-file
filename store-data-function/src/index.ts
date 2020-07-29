import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { StorageClient } from './libs/storage-client';
import { FirestoreClient } from './libs/firestore-client';
import { isNil } from './libs/utils';
import { StoreDataHandler } from './handler';
import config from './config';

const logger = pino({ level: 'info' });
const storage = new Storage();
const firestore = new Firestore();
const storageClient = new StorageClient(storage, { bucketName: config.CSV_BUCKET_NAME });
const firestoreClient = new FirestoreClient(firestore, { collectionName: config.DATASTORE_COLLECTION_NAME });
const statisticFirestoreClient = new FirestoreClient(firestore, {
  collectionName: config.STATISTIC_DATASTORE_COLLECTION_NAME,
});
const handler = new StoreDataHandler(
  logger,
  storageClient,
  firestoreClient,
  statisticFirestoreClient,
);

export const storeDataFunction = async (
  message: { data: string },
  context: Context,
  callback: Function,
): Promise<void> => {
  const startDate = new Date();

  if (isNil(message.data)) {
    return callback(null, null);
  }

  const payload = Buffer.from(message.data, 'base64').toString();
  const { outputFilename } = JSON.parse(payload);

  logger.info('Started Function', { filename: outputFilename, bucketName: config.SRC_CSV_BACKET });

  handler
    .handle(outputFilename)
    .then((result) => {
      if (result.isLeft()) {
        logger.error('Handler Error', result.value);

        return callback(result.value, { success: false });
      }
      const endDate = new Date();
      logger.info(`Finished Function in ${(endDate.getTime() - startDate.getTime()) / 1000} for ${outputFilename}`);
      callback(null, { success: true });
    })
    .catch((error) => {
      logger.error('Handler Error', error);
      callback(error, { success: false });
    });
};
