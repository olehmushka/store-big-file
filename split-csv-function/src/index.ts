import { Firestore } from '@google-cloud/firestore';
import { Storage } from '@google-cloud/storage';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { FirestoreClient } from './libs/firestore-client';
import { StorageClient } from './libs/storage-client';
import { SplitCsvFile } from './hanlder';
import config from './config';

type BaseFunctionInputData = {};

interface ISplitCSVDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const logger = pino({ level: 'info' });
const firestore = new Firestore();
const storage = new Storage();

export const splitCsvFunction = async (
  file: ISplitCSVDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  if (!file.name.includes('input')) {
    return callback(null, null);
  }
  const startDate = new Date();
  const inputBucketName = file.bucket;
  const outputBucketName = config.OUTPUT_CSV_BACKET;
  const details = {
    filename: file.name,
    inputBucketName: inputBucketName,
    outputBucketName: outputBucketName,
  };

  logger.info('Started Function', { filename: file.name, bucketName: inputBucketName });

  new SplitCsvFile(
    logger,
    new StorageClient(storage, { bucketName: inputBucketName }),
    new StorageClient(storage, { bucketName: outputBucketName }),
    new FirestoreClient(firestore, { collectionName: config.STATISTIC_DATASTORE_COLLECTION_NAME }),
  )
    .handle(file.name)
    .then((result) => {
      const endDate   = new Date();
      const duration = (endDate.getTime() - startDate.getTime()) / 1000;

      return result
        .mapLeft((error) => {
          logger.error(`Function is failed in ${duration}`, details, error);
          callback(error, { success: false });
        })
        .mapRight((count) => {
          logger.info(`Function is succeed in ${duration} for ${count} files`, details);
          callback(null, { success: true });
        });
    })
    .catch((error) => {
      logger.error('Function Error:', details, error);
      callback(error, { success: false });
    });
};
