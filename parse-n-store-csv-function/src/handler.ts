import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { IStorageClient } from './libs/storage-client';
import { IDatastoreClient } from './libs/datastore-client';
import { IUser } from './libs/contracts';

export class ParseNStoreCsvHandler {
  constructor(
    private logger: any,
    private storageClient: IStorageClient,
    private datastoreClient: IDatastoreClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error | unknown, void>> {
    const loadDate = new Date();

    return this.getDataFromCsvStream(this.storageClient.createFileReadStream(filename), loadDate.toISOString())
      .then(async (parsedResult) => {
        if (parsedResult.isLeft()) {
          this.logger.error(parsedResult.value);

          return left(parsedResult.value);
        }
        this.logger.info(`Parsed Result: length = ${parsedResult.value.length}`);

        return await this.datastoreClient.upsertLargeBulk<IUser>(parsedResult.value);
      });
  }

  private getDataFromCsvStream(stream: Readable, loadDate: string): Promise<Either<Error, IUser[]>> {
    return new Promise((resolve) => {
      const result: IUser[] = [];
      stream
        .pipe(csvParser())
        .on('error', (error) => resolve(left(error)))
        .on('data', (data: { email: string; eligible: boolean }) => result.push({ ...data, loadDate }))
        .on('end', () => {
          resolve(right(result));
        });
    });
  }
}
