import { Storage } from '@google-cloud/storage';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { StorageClient } from './libs/storage-client';
import { DeleteFileHandler } from './handler';
import config from './config';

type BaseFunctionInputData = {};

interface IStoreDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const logger = pino({ level: 'info' });
const storage = new Storage();

export const deleteFileFunction = async (
  file: IStoreDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  if (!file.name.includes('output')) {
    return callback(null, null);
  }
  const startDate = new Date()

  new DeleteFileHandler(logger, new StorageClient(storage, { bucketName: config.CSV_BUCKET_NAME }))
    .handle(file.name)
    .then((result) => {
      if (result.isLeft()) {
        logger.error('Handler Error', result.value);
  
        return callback(result.value, { success: false });
      }
      const endDate   = new Date();
      logger.info(`Finished Function in ${(endDate.getTime() - startDate.getTime()) / 1000}`);
      callback(null, { success: true });
    })
    .catch((error) => {
      logger.error('Handler Error', error);
      callback(error, { success: false });
    });
};
