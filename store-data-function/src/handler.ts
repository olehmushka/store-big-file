import { Readable } from 'stream';
import csvParser from 'csv-parser';
import { IStorageClient } from '../../libs/storage-client';
import { IDatastoreClient } from '../../libs/datastore-client';
import { ICSVPubSubPayload, IUser } from '../../libs/contracts';
import { BadPubSubPayload } from '../../libs/errors';

export class StoreDataHandler {
  constructor(private storageClient: IStorageClient, private datastoreClient: IDatastoreClient) {}

  public async handle(input?: string | null): Promise<void> {
    if (!input) {
      throw new BadPubSubPayload({ message: 'Data is empty' });
    }

    const csvFilename = this.getCsvFilename(input);
    if (csvFilename === '') {
      throw new BadPubSubPayload({ message: 'CSV filename is empty' });
    }

    const csvStream = this.storageClient.createFileReadStream(csvFilename);
    const parsedResult = await this.getDataFromCsvStream(csvStream);

    await this.storageClient.deleteFile(csvFilename);
    await this.datastoreClient.saveBulk<IUser>(parsedResult);
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

  private getDataFromCsvStream(stream: Readable): Promise<IUser[]> {
    return new Promise((resolve, reject) => {
      const result: IUser[] = [];
      stream
        .pipe(csvParser())
        .on('error', reject)
        .on('data', (data: IUser) => result.push(data))
        .on('end', () => {
          resolve(result);
        });
    });
  }
}
