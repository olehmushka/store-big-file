import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { IStorageClient } from './libs/storage-client';
import { IFirestoreClient } from './libs/firestore-client';
import { UploadStream } from './libs/upload-stream';
import { IUser, ICsvUploadStreamResult } from './libs/contracts';

export class ParseNStoreCsvHandler {
  constructor(
    private logger: any,
    private storageClient: IStorageClient,
    private firestoreClient: IFirestoreClient,
  ) {}

  public async handle(filename: string): Promise<Either<Error | unknown, void>> {
    const loadDate = new Date().toISOString();

    // return this.getDataFromCsvStream(this.storageClient.createFileReadStream(filename), loadDate)
    return this.uploadDataFromCsvStream(this.storageClient.createFileReadStream(filename), loadDate)
      .then(async (parsedResult) => {
        if (parsedResult.isLeft()) {
          this.logger.error(parsedResult.value);

          return left(parsedResult.value);
        }
        this.logger.info(`Parsed Result: length = ${parsedResult.value}`);

        return right(undefined);
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

  private uploadDataFromCsvStream(stream: Readable, loadDate: string): Promise<Either<Error, ICsvUploadStreamResult>> {
    return new Promise((resolve, reject) => {
      let recordCount = 0;

      stream
        .on('error', (error) => reject(left(error)))
        .pipe(csvParser())
        .on('error', (error) => reject(left(error)))
        .pipe(new UploadStream(this.firestoreClient, { batchSize: 500, parallelCommitSize: 500, loadDate }))
        .on('data', () => {
          recordCount += 1;
        })
        .on('end', () => {
          resolve(right({ recordCount }))
        });
    });
  }
}
