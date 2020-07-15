import { Storage } from '@google-cloud/storage';
import { Datastore } from '@google-cloud/datastore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import csvParser from 'csv-parser';

import { BadPubSubPayload } from './errors';
import config from './config';

interface ICSVPubSubPayload {
  data: {
    csvFilename: string;
  };
}

interface IUser {
  email: string;
  isOkay: boolean;
}

type BaseFunctionInputData = {};

interface IStoreDataInput extends BaseFunctionInputData {
  data?: string | null;
  messageId: string;
}

export const storeDataFunction = (pubSubMessage: IStoreDataInput, context: Context, callback: Function): void => {
  if (pubSubMessage.data === null || pubSubMessage.data === undefined) {
    return callback(new BadPubSubPayload({
      message: 'Data is empty',
      subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }

  let csvFilename = '';
  try {
    const payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString()) as ICSVPubSubPayload;
    csvFilename = payload?.data?.csvFilename ?? '';
  } catch {
    return callback(new BadPubSubPayload({
      message: 'Can not parse payload',
      subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }

  if (csvFilename === '') {
    return callback(new BadPubSubPayload({
      message: 'CSV filename is empty',
      subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }
  
  const storageClient = new Storage()
  const datastoreClient = new Datastore();
  const result: IUser[] = [];
  storageClient.bucket(config.CSV_BUCKET_NAME).file(csvFilename).createReadStream()
    .pipe(csvParser())
    .on('error', (error) => callback(error))
    .on('data', (data: IUser) => result.push(data))
    .on('end', async () => {
      try {
        await storageClient.bucket(config.CSV_BUCKET_NAME).file(csvFilename).delete();
        const key = datastoreClient.key(config.DATASTORE_COLLECTION_NAME);
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
