import { Datastore } from '@google-cloud/datastore';
import { Either, right, left } from '@sweet-monads/either';

export interface IDatastoreClient {
  saveBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  updateBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  upsertBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
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

  public saveBulk<T>(records: T[]): Promise<Either<Error | unknown, void>> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (error) {
          return resolve(left(error));
        }

        records.forEach((item) => {
          transaction.save({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!error) {
            return resolve(left(error));
          }
          resolve(right(undefined));
        });
      });
    });
  }

  public updateBulk<T>(records: T[]): Promise<Either<Error | unknown, void>> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (error) {
          return resolve(left(error));
        }

        records.forEach((item) => {
          transaction.update({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!error) {
            return resolve(left(error));
          }
          resolve(right(undefined));
        });
      });
    });
  }

  public upsertBulk<T>(records: T[]): Promise<Either<Error | unknown, void>> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (error) {
          return resolve(left(error));
        }

        records.forEach((item) => {
          transaction.upsert({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!error) {
            return resolve(left(error));
          }
          resolve(right(undefined));
        });
      });
    });
  }
}

export const getDatastoreClient = (
  instance: Datastore,
  datastoreConfig: IDatastoreClientConfig,
): IDatastoreClient => new DatastoreClient(instance, datastoreConfig);
