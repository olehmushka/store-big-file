import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from './libs/logger';
import { IFirestoreClient } from './libs/firestore-client';
import { IStorageClient } from './libs/storage-client';
import config from './config';
import { IBlacklistStatisticItem } from './libs/contracts';

const csvSplitStream = require('csv-split-stream');

export class SplitCsvFile {
  constructor(
    private logger: ILogger,
    private storageClient: IStorageClient,
    private statisticFirestoreClient: IFirestoreClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error | unknown, number>> {
    const timestamp = Date.now();
    const loadDate = new Date(timestamp).toISOString()
    let count = 0;
    const chunkFilenames: string[] = [];

    return csvSplitStream.split(
      this.storageClient.createFileReadStream(filename),
      { lineLimit: config.CSV_CHUNK_SIZE },
      (index: number) => {
        count = index + 1;
        const chunkFilename = `output-${timestamp}-${index}.csv`;
        chunkFilenames.push(chunkFilename);

        return this.storageClient.createFileWriteStream(chunkFilename);
      },
    )
      .then(async () => {
        await this.statisticFirestoreClient
          .setBulk<IBlacklistStatisticItem>(
            chunkFilenames.map(filename => ({ filename, state: 'pending', loadDate })),
            'filename',
          );

        return right(count);
      })
      .catch((error: unknown) => left(error));
  }
}
