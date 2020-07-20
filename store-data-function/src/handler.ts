import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { Either, left, right } from '@sweet-monads/either';
import { ILogger } from '../../libs/logger';
import { IStorageClient } from '../../libs/storage-client';
import { IDatastoreClient } from '../../libs/datastore-client';
import { ICSVPubSubPayload, IUser } from '../../libs/contracts';
import { BadPubSubPayload } from '../../libs/errors';

export class StoreDataHandler {
  constructor(
    private logger: ILogger,
    private storageClient: IStorageClient,
    private datastoreClient: IDatastoreClient,
  ) {}

  public async handle(input?: string | null): Promise<Either<BadPubSubPayload | Error, void>> {
    if (!input) {
      return left(new BadPubSubPayload({ message: 'Data is empty' }));
    }

    const csvFilename = this.getCsvFilename(input);
    if (csvFilename === '') {
      return left(new BadPubSubPayload({ message: 'CSV filename is empty' }));
    }

    const csvStream = this.storageClient.createFileReadStream(csvFilename);
    const parsedResult = await this.getDataFromCsvStream(csvStream);
    if (parsedResult.isLeft()) {
      return left(parsedResult.value);
    }

    await this.storageClient.deleteFile(csvFilename);
    await this.datastoreClient.saveBulk<IUser>(parsedResult.value);

    return right(undefined);
  }

  private parsePubSubMessage(input: string): ICSVPubSubPayload {
    return JSON.parse(Buffer.from(input, 'base64').toString());
  }

  private getCsvFilename(input: string): string {
    let csvFilename = '';
    try {
      const payload = this.parsePubSubMessage(input);
      csvFilename = payload?.data?.csvFilename ?? '';
    } catch {
      throw new BadPubSubPayload({ message: 'Can not parse payload' });
    }

    return csvFilename;
  }

  private getDataFromCsvStream(stream: Readable): Promise<Either<Error, IUser[]>> {
    return new Promise((resolve, reject) => {
      const result: IUser[] = [];
      stream
        .pipe(csvParser())
        .on('error', (error) => reject(left(error)))
        .on('data', (data: IUser) => result.push(data))
        .on('end', () => {
          resolve(right(result));
        });
    });
  }
}
