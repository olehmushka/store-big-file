import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { StorageClient } from './libs/storage-client';
import { FirestoreClient } from './libs/firestore-client';
import { StoreDataHandler } from './handler';
import config from './config';

type BaseFunctionInputData = {};

interface IStoreDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const logger = pino({ level: 'info' });
const storage = new Storage();
const firestore = new Firestore();
const storageClient = new StorageClient(storage, { bucketName: config.CSV_BUCKET_NAME });
const firestoreClient = new FirestoreClient(firestore, { collectionName: config.DATASTORE_COLLECTION_NAME });
const statisticFirestoreClient = new FirestoreClient(firestore, {
  collectionName: config.STATISTIC_DATASTORE_COLLECTION_NAME,
});
const handler = new StoreDataHandler(logger, storageClient, firestoreClient, statisticFirestoreClient);

export const storeDataFunction = async (file: IStoreDataInput, context: Context, callback: Function): Promise<void> => {
  if (!file.name.includes('output')) {
    return callback(null, null);
  }
  const startDate = new Date();

  logger.info('Started Function', { filename: file.name, bucketName: file.bucket });

  handler
    .handle(file.name)
    .then((result) => {
      if (result.isLeft()) {
        logger.error('Handler Error', result.value);

        return callback(result.value, { success: false });
      }
      const endDate = new Date();
      logger.info(`Finished Function in ${(endDate.getTime() - startDate.getTime()) / 1000} for ${file.name}`);
      callback(null, { success: true });
    })
    .catch((error) => {
      logger.error('Handler Error', error);
      callback(error, { success: false });
    });
};
