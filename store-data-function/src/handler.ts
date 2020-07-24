import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from './libs/logger';
import { IStorageClient } from './libs/storage-client';
import { IFirestoreClient } from './libs/firestore-client';
import { IBlacklistStatisticItem, ICsvUploadStreamResult } from './libs/contracts';
import { UploadStream } from './libs/upload-stream';
import config from './config';

export class StoreDataHandler {
  constructor(
    private logger: ILogger,
    private storageClient: IStorageClient,
    private blacklistFirestoreClient: IFirestoreClient,
    private statisticFirestoreClient: IFirestoreClient,
  ) {}

  public async handle(csvFilename: string): Promise<Either<Error, void>> {
    if (csvFilename === '') {
      return left(new Error('CSV filename is empty'));
    }
    const splitCsvFilename = csvFilename.split('-');
    if (splitCsvFilename.length !== 3) {
      return this.sendRejectedStatistics({ filename: csvFilename })
        .then((result) => {
          if (result.isLeft()) {
            return result;
          }

          return left(new Error('CSV filename is incorrect'));
        });
    }
    const loadDate = new Date(Number(splitCsvFilename[1])).toISOString();

    const csvStream = this.storageClient.createFileReadStream(csvFilename);
    const result = await this.uploadDataFromCsvStream(csvStream, loadDate);
    if (result.isLeft()) {
      return this.sendRejectedStatistics({ filename: csvFilename, loadDate })
        .then((statisticExecResult) => {
          if (statisticExecResult.isLeft()) {
            return statisticExecResult;
          }

          return left(result.value as Error);
        });
    }

    // await this.blacklistFirestoreClient.setLargeBulk<IUser>(parsedResult.value, 'email');
    this.logger.info({ countStored: result.value });
    await this.storageClient.deleteFile(csvFilename);
    this.logger.info(`Deleted ${csvFilename} file`);

    return this.sendFulfilledStatistics({ filename: csvFilename, loadDate });
  }

  private uploadDataFromCsvStream(stream: Readable, loadDate: string): Promise<Either<Error, ICsvUploadStreamResult>> {
    return new Promise((resolve, reject) => {
      let recordCount = 0;

      stream
        .on('error', (error) => reject(left(error)))
        .pipe(csvParser())
        .on('error', (error) => reject(left(error)))
        .pipe(new UploadStream(this.blacklistFirestoreClient, {
          batchSize: config.BATCH_SIZE,
          parallelCommitSize: config.PARALLEL_BATCHES_NUMBER,
          loadDate,
        }))
        .on('data', () => {
          recordCount += 1;
        })
        .on('end', () => {
          resolve(right({ recordCount }))
        });
    });
  }

  private sendFulfilledStatistics(data: { filename: string; loadDate?: string }): Promise<Either<Error, void>> {
    return this.statisticFirestoreClient.set<IBlacklistStatisticItem>({
      filename: data.filename,
      state: 'fulfilled',
      loadDate: data.loadDate,
    }, 'filename');
  }

  private sendRejectedStatistics(data: { filename: string; loadDate?: string }): Promise<Either<Error, void>> {
    return this.statisticFirestoreClient.set<IBlacklistStatisticItem>({
      filename: data.filename,
      state: 'rejected',
      loadDate: data.loadDate ?? '',
    }, 'filename');
  }
}
