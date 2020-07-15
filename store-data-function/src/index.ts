import { Storage } from '@google-cloud/storage';
import { Datastore } from '@google-cloud/datastore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';

import { StorageClient } from '../../libs/storage-client';
import { DatastoreClient } from '../../libs/datastore-client';
import { StoreDataHandler } from './handler';
import config from './config';

type BaseFunctionInputData = {};

interface IStoreDataInput extends BaseFunctionInputData {
  data?: string | null;
  messageId: string;
}

export const storeDataFunction = async (
  pubSubMessage: IStoreDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  const handler = new StoreDataHandler(
    new StorageClient(new Storage(), { bucketName: config.CSV_BUCKET_NAME }),
    new DatastoreClient(new Datastore(), { collectionName: config.DATASTORE_COLLECTION_NAME }),
  );

  try {
    await handler.handle(pubSubMessage.data);
    callback(null, { success: true });
  } catch (error) {
    callback(
      {
        ...error,
        subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
        messageId: pubSubMessage.messageId,
      },
      { success: false },
    );
  }
};
