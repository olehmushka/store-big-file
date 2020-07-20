import { Storage } from '@google-cloud/storage';
import { Datastore } from '@google-cloud/datastore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { StorageClient } from '../../libs/storage-client';
import { DatastoreClient } from '../../libs/datastore-client';
import { StoreDataHandler } from './handler';
import config from './config';

type BaseFunctionInputData = {};

interface IStoreDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const logger = pino({ level: 'info' });

export const storeDataFunction = async (
  file: IStoreDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  if (!file.name.includes('input')) {
    return callback(null, null);
  }
  const startDate = new Date()

  const handler = new StoreDataHandler(
    logger,
    new StorageClient(new Storage(), { bucketName: config.CSV_BUCKET_NAME }),
    new DatastoreClient(new Datastore(), { collectionName: config.DATASTORE_COLLECTION_NAME }),
  );

  try {
    const result = await handler.handle(file.name);
    if (result.isLeft()) {
      logger.error('Handler Error', result.value);

      return callback(result.value, { success: false });
    }
    const endDate   = new Date();
    logger.info(`Finished Function in ${(endDate.getTime() - startDate.getTime()) / 1000}`);
    callback(null, { success: true });
  } catch (error) {
    logger.error('Handler Error', error);
    callback(error, { success: false });
  }
};
