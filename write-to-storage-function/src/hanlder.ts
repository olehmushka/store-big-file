import { Either, left, right } from '@sweet-monads/either';
import { Writable } from 'stream';
import { ILogger } from './libs/logger';
import { IBlacklistStatisticItem } from './libs/contracts';
import { IStorageClient } from './libs/storage-client';
import { IFirestoreClient } from './libs/firestore-client';
import * as csvSplitStream from './libs/spliter';
import config from './config';

export class WriteToStorage {
  constructor(
    private logger: ILogger,
    private inputStorageClient: IStorageClient,
    private outputStorageClient: IStorageClient,
    private statisticFirestoreClient: IFirestoreClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error, void>> {
    const timestamp = Date.now();

    try {
      const outputFilenames = await this.split(filename, timestamp);
      if (outputFilenames.isLeft()) {
        return left(outputFilenames.value);
      }

      return this.saveStatistic(outputFilenames.value, timestamp);
    } catch (error) {
      this.logger.error('Handler Error:', { error });

      return left(error as Error);
    }
  }

  private pipeWrite(inputFilename: string, outputFilename: string): Promise<Either<Error, void>> {
    return new Promise((resolve) => {
      this.inputStorageClient.createFileReadStream(inputFilename)
        .pipe(this.outputStorageClient.createFileWriteStream(outputFilename))
        .on('error', (error) => {
          resolve(left(error as Error));
        })
        .on('end', () => {
          resolve(right(undefined));
        });
    });
  }

  private async split(inputFilename: string, timestamp: number): Promise<Either<Error, string[]>> {
    const chunkFilenames: string[] = [];

    const inputStream = this.inputStorageClient.createFileReadStream(inputFilename);
    const inputSplitOptions = { lineLimit: config.CSV_CHUNK_SIZE };
    const createOutputStreamCallback = (index: number): Writable => {
      const chunkFilename = `output-${timestamp}-${index}.csv`;
      chunkFilenames.push(chunkFilename);

      return this.outputStorageClient.createFileWriteStream(chunkFilename);
    };

    try {
      await csvSplitStream.split(inputStream, inputSplitOptions, createOutputStreamCallback);

      return right(chunkFilenames);
    } catch (error) {
      return left(error as Error);
    }
  }

  private saveStatistic(filenames: string[], timestamp: number): Promise<Either<Error, void>> {
    const loadDate = new Date(timestamp).toISOString();
    const transformCallback = (filename: string): IBlacklistStatisticItem => ({
      filename,
      state: 'pending',
      loadDate,
    });

    return this.statisticFirestoreClient
      .setBulk<IBlacklistStatisticItem>(filenames.map(transformCallback), 'filename');
  }
}
