import { Storage } from '@google-cloud/storage';
import { Context } from '@google-cloud/functions-framework/build/src/functions';
import pino from 'pino';

import { StorageClient } from './libs/storage-client';
import { SplitCsvFile } from './hanlder';

type BaseFunctionInputData = {};

interface ISplitCSVDataInput extends BaseFunctionInputData {
  bucket: string;
  name: string;
}

const logger = pino({ level: 'info' });
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

  logger.info('Started Function', { filename: file.name, bucketName: file.bucket });

  new SplitCsvFile(logger, new StorageClient(storage, { bucketName: file.bucket }))
    .handle(file.name)
    .then((result) => {
      const endDate   = new Date();
      const duration = (endDate.getTime() - startDate.getTime()) / 1000;
      const details = {
        filename: file.name,
        bucketName: file.bucket,
      };

      return result
        .mapLeft((error) => {
          logger.error(`Function is failed in ${duration}`, details, error);
          callback(error, { success: false });
        })
        .mapRight(() => {
          logger.error(`Function is succeed in ${duration}`, details);
          callback(null, { success: true });
        });
    })
    .catch(error => callback(error, { success: false }));
};
