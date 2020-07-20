import { PubSub } from '@google-cloud/pubsub';
import { Storage } from '@google-cloud/storage';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { StorageClient } from '../../libs/storage-client';
import { PubSubClient } from '../../libs/pubsub-client';
import { SplitCsvFile } from './hanlder';
import config from './config';

type BaseFunctionInputData = {};

interface ISplitCSVDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

export const splitCsvFunction = async (
  file: ISplitCSVDataInput,
  context: Context,
  callback: Function,
): Promise<void> => {
  if (!file.name.includes('input')) {
    return callback(null, null);
  }

  try {
    const handler = new SplitCsvFile(
      pino({ level: 'info' }),
      new StorageClient(new Storage(), { bucketName: file.bucket }),
      new PubSubClient(new PubSub(), {
        topicName: `projects/${config.PROJECT_ID}/topics/${config.SPLITTED_CSV_FILE_TOPIC_NAME}`,
      }),
    );
    const result = await handler.handle(file.name);
    result
      .mapLeft((error) => callback(error, { success: false }))
      .mapRight((messageIDs) => callback(null, { success: true, messageIDs }));
  } catch (error) {
    callback(error, { success: false });
  }
};
