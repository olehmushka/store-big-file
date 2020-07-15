import { PubSub } from '@google-cloud/pubsub';
import { Storage } from '@google-cloud/storage';
import { Context } from '@google-cloud/functions-framework/build/src/functions';

import { StorageClient } from '../../libs/storage-client';
import { PubSubClient } from '../../libs/pubsub-client';
import { ICSVPubSubPayload } from '../../libs/contracts';
import config from './config';

const csvSplitStream = require('csv-split-stream');

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
  const storageClient = new StorageClient(new Storage(), { bucketName: file.bucket });
  const pubSubClient = new PubSubClient(new PubSub(), {
    topicName: `projects/${config.PROJECT_ID}/topics/${config.SPLITTED_CSV_FILE_TOPIC_NAME}`,
  });
  const timestamp = Date.now();
  const outputFilenames: string[] = [];

  const readFileStream = storageClient.createFileReadStream(file.name);
  try {
    await csvSplitStream.split(readFileStream, { lineLimit: 100 }, (index: number) => {
      const outputFilename = `output-${timestamp}-${index}.csv`;
      outputFilenames.push(outputFilename);

      return storageClient.createFileWriteStream(outputFilename);
    });
    const publishedMessageIDs = await Promise.all(
      outputFilenames.map((filename) =>
        pubSubClient.publish(JSON.stringify({ data: { csvFilename: filename } } as ICSVPubSubPayload)),
      ),
    );
    callback(null, { success: true, publishedMessageIDs });
  } catch (error) {
    callback(error, { success: false });
  }
};
