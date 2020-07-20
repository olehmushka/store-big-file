import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from './libs/logger';
import { IStorageClient } from './libs/storage-client';
import config from './config';

const csvSplitStream = require('csv-split-stream');

export class SplitCsvFile {
  constructor(
    private logger: ILogger,
    private storageClient: IStorageClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error | unknown, undefined>> {
    const timestamp = Date.now();

    return csvSplitStream.split(
      this.storageClient.createFileReadStream(filename),
      { lineLimit: config.CSV_CHUNK_SIZE },
      (index: number) => this.storageClient.createFileWriteStream(`output-${timestamp}-${index}.csv`),
    )
      .then(() => right(undefined))
      .catch((error: unknown) => left(error));
  }
}
