import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from './libs/logger';
import { IStorageClient } from './libs/storage-client';
import { IFirestoreClient } from './libs/firestore-client';
import { IBlacklistStatisticItem } from './libs/contracts';
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
      return this.sendRejectedStatistics({ filename: csvFilename })
        .then((statisticExecResult) => {
          if (statisticExecResult.isLeft()) {
            return statisticExecResult;
          }

          return left(result.value as Error);
        });
    }
    this.logger.info(`Stored ${result.value} records into Firestore`);

    const deletionOutputFileResult = await this.storageClient.deleteFile(csvFilename);
    if (deletionOutputFileResult.isLeft()) {
      return deletionOutputFileResult;
    }
    this.logger.info(`Deleted ${csvFilename} file`);

    const sendStatisticResult = await this.sendFulfilledStatistics({
      filename: csvFilename,
      loadDate,
      storedByItemCount: result.value,
    });
    if (sendStatisticResult.isLeft()) {
      return sendStatisticResult;
    }

    // const statisticReport = await this.getStatisticReport(loadDate);
    // if (statisticReport.isLeft()) {
    //   return left(statisticReport.value);
    // }
    // const { isFinished, isSucceed } = statisticReport.value;
    // this.logger.info(
    //   `Statistic report for ${csvFilename} round isFinished=${isFinished}, isSucceed=${isSucceed}`,
    // );
    // if (isFinished && isSucceed) {
    //   return this.commitFinish(loadDate);
    // }

    return right(undefined);
  }

  private uploadDataFromCsvStream(stream: Readable, loadDate: string): Promise<Either<Error, number>> {
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
          resolve(right(recordCount))
        });
    });
  }

  // private async getStatisticReport(loadData: string): Promise<Either<Error, IStatisticReport>> {
  //   const outputFilenames = await this.statisticFirestoreClient.get<IBlacklistStatisticItem>({
  //     fieldName: 'loadDate',
  //     operation: '==',
  //     valueToCompare: loadData,
  //   });
  //   if (outputFilenames.isLeft()) {
  //     return left(outputFilenames.value);
  //   }

  //   if (outputFilenames.value.length === 0) {
  //     return left(new Error('blacklist statistic collection is empty'));
  //   }

  //   return right({
  //     isFinished: outputFilenames.value.every(filename => filename.state !== 'pending'),
  //     isSucceed: outputFilenames.value.every(filename => filename.state === 'fulfilled'),
  //     srcFilename: outputFilenames.value[0].srcFilename ?? '',
  //   });
  // }

  // private async commitFinish(loadData: string): Promise<Either<Error, void>> {
  //   const outputFilenames = await this.statisticFirestoreClient.get<IBlacklistStatisticItem>({
  //     fieldName: 'loadDate',
  //     operation: '==',
  //     valueToCompare: loadData,
  //   });
  //   if (outputFilenames.isLeft()) {
  //     return left(outputFilenames.value);
  //   }

  //   if (outputFilenames.value.length === 0) {
  //     return left(new Error('blacklist statistic collection is empty'));
  //   }

  //   const [firstOutputFilename] = outputFilenames.value;
  //   if (firstOutputFilename === undefined) {
  //     return left(new Error(`output filenames were not found for load date is ${loadData}`));
  //   }
  //   const { srcFilename } = firstOutputFilename;
  //   if (srcFilename === undefined || srcFilename === '') {
  //     return left(new Error(`source filename is undefined for load date is ${loadData}`));
  //   }
  //   const fileExtension = '.csv';
  //   const [filenameBase] = srcFilename.split(fileExtension);
  //   const currentSourceFilename = `${filenameBase}${config.PENDING_FILE_SUFFIX}${fileExtension}`;
  //   const finishedSourceFilename = `${filenameBase}${config.FINISHED_FILE_SUFFIX}${fileExtension}`;

  //   const renamingSourceFileResult = await this.srcStorageClient.renameFile(
  //     currentSourceFilename,
  //     finishedSourceFilename,
  //   );
  //   if (renamingSourceFileResult.isLeft()) {
  //     return renamingSourceFileResult;
  //   }
  //   this.logger.info(
  //     `Renamed ${currentSourceFilename} to ${finishedSourceFilename} in ${config.SRC_CSV_BACKET} bucket`,
  //   );

  //   return right(undefined);
  // }

  private sendFulfilledStatistics(
    data: { filename: string; loadDate: string; storedByItemCount: number },
  ): Promise<Either<Error, void>> {
    return this.statisticFirestoreClient.set<IBlacklistStatisticItem>({
      filename: data.filename,
      state: 'fulfilled',
      loadDate: data.loadDate,
      storedByItemCount: data.storedByItemCount,
    }, 'filename');
  }

  private sendRejectedStatistics(data: { filename: string }): Promise<Either<Error, void>> {
    return this.statisticFirestoreClient.set<IBlacklistStatisticItem>({
      filename: data.filename,
      state: 'rejected',
    }, 'filename');
  }
}
