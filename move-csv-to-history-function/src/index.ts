import { Firestore } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { FirestoreClient } from './libs/firestore-client';
import { getStorageClient } from './libs/storage-client';
import { MoveCsvToHistoryHandler } from './handler';
import config from './config';


type BaseFunctionInputData = {};

interface IMoveCsvToHistoryDataInput extends BaseFunctionInputData {
  name: string;
}

const logger = pino({ level: 'info' });
const firestore = new Firestore();
const storage = new Storage();
const srcStorageClient = getStorageClient(storage, { bucketName: config.SRC_CSV_BACKET });
const handler = new MoveCsvToHistoryHandler(
  logger,
  new FirestoreClient(firestore, { collectionName: config.STATISTIC_DATASTORE_COLLECTION_NAME }),
  srcStorageClient,
);

export const moveCsvToHistoryFunction = async (
  file: IMoveCsvToHistoryDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  const shouldSkip = !file.name.includes('.csv')
    || file.name.includes(config.IGNORED_FILE_SUFFIX);
  if (shouldSkip) {
    logger.warn('Skipped file handling', { filename: file.name });

    return callback(null, null);
  }
  const startDate = new Date();

  logger.info('Started Function');

  handler
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
