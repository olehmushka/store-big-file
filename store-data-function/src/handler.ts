import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from './libs/logger';
import { IStorageClient } from './libs/storage-client';
import { IFirestoreClient } from './libs/firestore-client';
import { IUser, IBlacklistStatisticItem } from './libs/contracts';

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
    const parsedResult = await this.getDataFromCsvStream(csvStream, loadDate);
    if (parsedResult.isLeft()) {
      return this.sendRejectedStatistics({ filename: csvFilename, loadDate })
        .then((result) => {
          if (result.isLeft()) {
            return result;
          }

          return left(parsedResult.value as Error);
        });
    }

    await this.blacklistFirestoreClient.setLargeBulk<IUser>(parsedResult.value, 'email');
    await this.storageClient.deleteFile(csvFilename);

    return this.sendFulfilledStatistics({ filename: csvFilename, loadDate });
  }

  private getDataFromCsvStream(stream: Readable, loadDate: string): Promise<Either<Error, IUser[]>> {
    return new Promise((resolve, reject) => {
      const result: IUser[] = [];
      stream
        .pipe(csvParser())
        .on('error', (error) => reject(left(error)))
        .on('data', (data: { email: string; eligible: boolean }) => result.push({ ...data, loadDate }))
        .on('end', () => {
          resolve(right(result));
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
