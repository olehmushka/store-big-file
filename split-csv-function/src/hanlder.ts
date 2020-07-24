import { Either, left, right } from '@sweet-monads/either';
import { Writable } from 'stream';
import { ILogger } from './libs/logger';
import { IFirestoreClient } from './libs/firestore-client';
import { IStorageClient } from './libs/storage-client';
import config from './config';
import { IBlacklistStatisticItem } from './libs/contracts';

const csvSplitStream = require('csv-split-stream');

export class SplitCsvFile {
  constructor(
    private logger: ILogger,
    private inputStorageClient: IStorageClient,
    private outputStorageClient: IStorageClient,
    private statisticFirestoreClient: IFirestoreClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error, number>> {
    const timestamp = Date.now();
    const loadDate = new Date(timestamp).toISOString();
    const chunkFilenames: string[] = [];

    const inputStream = this.inputStorageClient.createFileReadStream(filename);
    const inputSplitOptions = { lineLimit: config.CSV_CHUNK_SIZE };
    const createOutputStreamCallback = (index: number): Writable => {
      const chunkFilename = `output-${timestamp}-${index}.csv`;
      chunkFilenames.push(chunkFilename);

      return this.outputStorageClient.createFileWriteStream(chunkFilename);
    };

    try {
      await csvSplitStream.split(inputStream, inputSplitOptions, createOutputStreamCallback);
      await this.statisticFirestoreClient.setBulk<IBlacklistStatisticItem>(
        chunkFilenames.map(filename => ({ filename, state: 'pending', loadDate })),
        'filename',
      );
      this.logger.info(`Created ${chunkFilenames.length} files`, { filenames: chunkFilenames });

      return right(chunkFilenames.length);
    } catch (error) {
      this.logger.error('Handler Error:', { error });

      return left(error as Error);
    }
  }
}
