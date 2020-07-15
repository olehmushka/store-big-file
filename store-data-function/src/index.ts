import { Storage } from '@google-cloud/storage';
import { Datastore } from '@google-cloud/datastore';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import csvParser from 'csv-parser';
import { Readable } from 'stream';

import { StorageClient } from '../../libs/storage-client';
import { DatastoreClient } from '../../libs/datastore-client';
import { ICSVPubSubPayload, IUser } from '../../libs/contracts';
import { BadPubSubPayload } from '../../libs/errors';
import config from './config';

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

  const storageClient = new StorageClient(new Storage(), { bucketName: config.CSV_BUCKET_NAME });
  const datastoreClient = new DatastoreClient(new Datastore(), { collectionName: config.DATASTORE_COLLECTION_NAME });

  try {
    const parsedResult = await parseCsvStream(storageClient.createFileReadStream(csvFilename));

    await storageClient.deleteFile(csvFilename);
    await datastoreClient.saveBulk<IUser>(parsedResult);
  } catch (error) {
    return callback(error, { success: false });
  }
};
