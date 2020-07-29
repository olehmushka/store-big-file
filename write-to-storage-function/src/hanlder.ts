import { Either, left, right } from '@sweet-monads/either';
import { Writable } from 'stream';
import csvParser from 'csv-parser';
import { ILogger } from './libs/logger';
import { IBlacklistStatisticItem } from './libs/contracts';
import { IStorageClient } from './libs/storage-client';
import { IFirestoreClient } from './libs/firestore-client';
import { IPubSubClient } from './libs/pubsub-client';
import * as csvSplitStream from './libs/spliter';
import config from './config';

export class WriteToStorage {
  constructor(
    private logger: ILogger,
    private historyStorageClient: IStorageClient,
    private outputStorageClient: IStorageClient,
    private statisticFirestoreClient: IFirestoreClient,
    private resultPubSubClient: IPubSubClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error, number>> {
    const timestamp = Date.now();

    try {
      const outputFilenames = await this.split(filename, timestamp);
      if (outputFilenames.isLeft()) {
        return left(outputFilenames.value);
      }

      const totalCount = await this.countCsvLines(filename);
      if (totalCount.isLeft()) {
        return left(totalCount.value);
      }

      await this.saveStatistic({
        filenames: outputFilenames.value,
        srcFilename: filename,
        totalCount: totalCount.value,
        timestamp,
      });

      return this.sendResult(outputFilenames.value);
    } catch (error) {
      this.logger.error('Handler Error:', { error });

      return left(error as Error);
    }
  }

  private async split(inputFilename: string, timestamp: number): Promise<Either<Error, string[]>> {
    const chunkFilenames: string[] = [];

    const historyBucketStream = this.historyStorageClient.createFileReadStream(inputFilename);
    const historySplitOptions = { lineLimit: config.CSV_CHUNK_SIZE };
    const createOutputStreamCallback = (index: number): Writable => {
      const chunkFilename = `output-${timestamp}-${index}.csv`;
      chunkFilenames.push(chunkFilename);

      return this.outputStorageClient.createFileWriteStream(chunkFilename);
    };

    try {
      await csvSplitStream.split(historyBucketStream, historySplitOptions, createOutputStreamCallback);

      return right(chunkFilenames);
    } catch (error) {
      return left(error as Error);
    }
  }

  private async countCsvLines(inputFilename: string): Promise<Either<Error, number>> {
    return new Promise((resolve) => {
      let count = 0;
      this.historyStorageClient.createFileReadStream(inputFilename)
        .pipe(csvParser())
        .on('error', (error) => resolve(left(error)))
        .on('data', () => {
          count += 1;
        })
        .on('end', () => {
          resolve(right(count));
        });
    });
  }

  private saveStatistic(
    data: { filenames: string[]; srcFilename: string; totalCount: number; timestamp: number },
  ): Promise<Either<Error, void>> {
    const loadDate = new Date(data.timestamp).toISOString();
    const transformCallback = (filename: string): IBlacklistStatisticItem => ({
      filename,
      state: 'pending',
      loadDate,
      srcFilename: data.srcFilename,
      storedByItemCount: 0,
      totalCount: data.totalCount,
    });

    return this.statisticFirestoreClient
      .setBulk<IBlacklistStatisticItem>(data.filenames.map(transformCallback), 'filename');
  }

  private sendResult(outputFilenames: string[]): Promise<Either<Error, number>> {
    return Promise.all(
      outputFilenames.map(async (outputFilename: string): Promise<Either<Error, void>> => {
        const publishResult = await this.resultPubSubClient.publish(JSON.stringify({ outputFilename }));
        if (publishResult.isLeft()) {
          return left(publishResult.value as Error);
        }
        this.logger.info(`Sent message(id=${publishResult.value}) for ${outputFilename} file`);

        return right(undefined);
      }),
    )
      .then((result) => {
        const [leftValue] = result.filter(line => line.isLeft());
        if (leftValue !== undefined) {
          return left(leftValue.value as Error);
        }

        return right(result.length);
      })
  }
}
