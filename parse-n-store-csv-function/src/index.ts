import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { getStorageClient } from './libs/storage-client';
import { FirestoreClient } from './libs/firestore-client';
import { ParseNStoreCsvHandler } from './handler';
import config from './config';


type BaseFunctionInputData = {};

interface IParseNStoreCsvDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const logger = pino({ level: 'info' });
const storage = new Storage();
const firestore = new Firestore();

export const parseNStoreCsvFunction = async (
  file: IParseNStoreCsvDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  if (!file.name.includes('input')) {
    logger.warn('Skipped file handling', { filename: file.name });

    return callback(null, null);
  }
  const startDate = new Date();

  logger.info('Started Function');

  new ParseNStoreCsvHandler(
    logger,
    getStorageClient(storage, { bucketName: file.bucket }),
    new FirestoreClient(firestore, { collectionName: config.DATASTORE_COLLECTION_NAME }),
  )
    .handle(file.name)
    .then((result) => {
      const endDate   = new Date();
      logger.info(`Finished Function in ${(endDate.getTime() - startDate.getTime()) / 1000}`);

      return result;
    })
    .then((result) => {
      if (result.isLeft()) {
        logger.error('Handler Error', result.value);
  
        return callback(result.value, { success: false });
      }
      callback(null, { success: true });
    })
    .catch((error) => {
      logger.error('Handler Error', error);
      callback(error, { success: false });
    });
};
