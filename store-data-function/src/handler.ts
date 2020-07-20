import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from '../../libs/logger';
import { IStorageClient } from '../../libs/storage-client';
import { IDatastoreClient } from '../../libs/datastore-client';
import { IUser } from '../../libs/contracts';
import { BadPubSubPayload } from '../../libs/errors';

export class StoreDataHandler {
  constructor(
    private logger: ILogger,
    private storageClient: IStorageClient,
    private datastoreClient: IDatastoreClient,
  ) {}

  public async handle(csvFilename: string): Promise<Either<BadPubSubPayload | Error, void>> {
    if (csvFilename === '') {
      return left(new BadPubSubPayload({ message: 'CSV filename is empty' }));
    }
    const splitCsvFilename = csvFilename.split('-');
    if (splitCsvFilename.length !== 3) {
      return left(new BadPubSubPayload({ message: 'CSV filename is incorrect' }));
    }
    const loadData = new Date(new Date(Number(splitCsvFilename[1]))).toISOString();

    const csvStream = this.storageClient.createFileReadStream(csvFilename);
    const parsedResult = await this.getDataFromCsvStream(csvStream, loadData);
    if (parsedResult.isLeft()) {
      return left(parsedResult.value);
    }

    await this.storageClient.deleteFile(csvFilename);
    await this.datastoreClient.upsertBulk<IUser>(parsedResult.value);

    return right(undefined);
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
}
