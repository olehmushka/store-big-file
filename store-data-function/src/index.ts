import { Storage } from '@google-cloud/storage';
import { Datastore } from '@google-cloud/datastore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import csvParser from 'csv-parser';
import { Readable } from 'stream';

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

const parsePubSubMessage = (input: IStoreDataInput): ICSVPubSubPayload =>
  JSON.parse(Buffer.from(input?.data ?? '', 'base64').toString());

const parseCsvStream = (stream: Readable): Promise<IUser[]> =>
  new Promise((resolve, reject) => {
    const result: IUser[] = [];
    stream
      .pipe(csvParser())
      .on('error', reject)
      .on('data', (data: IUser) => result.push(data))
      .on('end', () => {
        resolve(result);
      });
  });

export const storeDataFunction = async (
  pubSubMessage: IStoreDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  if (pubSubMessage.data === null || pubSubMessage.data === undefined) {
    return callback(
      new BadPubSubPayload({
        message: 'Data is empty',
        subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
        messageId: pubSubMessage.messageId,
      }),
      { success: false },
    );
  }

  let csvFilename = '';
  try {
    const payload = parsePubSubMessage(pubSubMessage);
    csvFilename = payload?.data?.csvFilename ?? '';
  } catch {
    return callback(
      new BadPubSubPayload({
        message: 'Can not parse payload',
        subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
        messageId: pubSubMessage.messageId,
      }),
      { success: false },
    );
  }

  if (csvFilename === '') {
    return callback(
      new BadPubSubPayload({
        message: 'CSV filename is empty',
        subscriptionName: config.SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
        messageId: pubSubMessage.messageId,
      }),
      { success: false },
    );
  }

  const storageClient = new Storage();
  const datastoreClient = new Datastore();

  let parsedResult: IUser[] = [];

  try {
    parsedResult = await parseCsvStream(
      storageClient.bucket(config.CSV_BUCKET_NAME).file(csvFilename).createReadStream(),
    );

    await storageClient.bucket(config.CSV_BUCKET_NAME).file(csvFilename).delete();
  } catch (error) {
    return callback(error, { success: false });
  }

  const key = datastoreClient.key(config.DATASTORE_COLLECTION_NAME);
  const transaction = datastoreClient.transaction();
  transaction.run((error) => {
    if (error) {
      return callback(error, { success: false });
    }

    parsedResult.forEach((item) => {
      transaction.save({ key, data: item });
    });

    transaction.commit((error) => {
      if (!error) {
        return callback(error, { success: false });
      }
      callback(null, { success: true });
    });
  });
};
