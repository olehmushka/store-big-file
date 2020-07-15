import { Datastore } from '@google-cloud/datastore';

export interface IDatastoreClient {
  saveBulk<T>(records: T[]): Promise<void>;
}

export interface IDatastoreClientConfig {
  collectionName: string;
}

export class DatastoreClient implements IDatastoreClient {
  private readonly instance: Datastore;
  private collectionName: string;

  constructor(instance: Datastore, datastoreConfig: IDatastoreClientConfig) {
    this.instance = instance;
    this.collectionName = datastoreConfig.collectionName;
  }

  public saveBulk<T>(records: T[]): Promise<void> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve, reject) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (error) {
          return reject(error);
        }

        records.forEach((item) => {
          transaction.save({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!error) {
            return reject(error);
          }
          resolve();
        });
      });
    });
  }
}
