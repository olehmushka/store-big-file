import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from './libs/logger';
import { IStorageClient } from './libs/storage-client';

export class DeleteFileHandler {
  constructor(
    private logger: ILogger,
    private storageClient: IStorageClient,
  ) {}

  public async handle(csvFilename: string): Promise<Either<Error, void>> {
    if (csvFilename === '') {
      return left(new Error('CSV filename is empty'));
    }
    const splitCsvFilename = csvFilename.split('-');
    if (splitCsvFilename.length !== 3) {
      return left(new Error('CSV filename is incorrect'));
    }
    const loadData = new Date(new Date(Number(splitCsvFilename[1]))).toISOString();

    await this.storageClient.deleteFile(csvFilename);
    this.logger.info(`Removed ${csvFilename} file for date: ${loadData}`);

    return right(undefined);
  }
}
