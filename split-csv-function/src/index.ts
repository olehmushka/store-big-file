import { Storage } from '@google-cloud/storage';
import { PubSub } from '@google-cloud/pubsub';
import { Context } from '@google-cloud/functions-framework/build/src/functions';

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
  const storageClient = new Storage();
  const pubSubClient = new PubSub();
  const timestamp = Date.now();
  const fullTopicName = `projects/${config.PROJECT_ID}/topics/${config.SPLITTED_CSV_FILE_TOPIC_NAME}`;
  const outputFilenames: string[] = [];

  const readFileStream = storageClient.bucket(file.bucket).file(file.name).createReadStream();
  try {
    await csvSplitStream.split(readFileStream, { lineLimit: 100 }, (index: number) => {
      const outputFilename = `output-${timestamp}-${index}.csv`;
      outputFilenames.push(outputFilename);

      return storageClient.bucket(file.bucket).file(outputFilename).createWriteStream();
    });
    const publishedMessageIDs = await Promise.all(
      outputFilenames.map((filename) => {
        const payload = JSON.stringify({ data: { csvFilename: filename } });

        return pubSubClient.topic(fullTopicName).publish(Buffer.from(payload));
      }),
    );
    callback(null, { success: true, publishedMessageIDs });
  } catch (error) {
    callback(error, { success: false });
  }
};
