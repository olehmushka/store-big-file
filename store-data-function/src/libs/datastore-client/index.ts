import { Datastore } from '@google-cloud/datastore';
import { Either, right, left } from '@sweet-monads/either';
import { chunk, isNil } from '../utils';

export interface IDatastoreClient {
  saveBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  saveLargeBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  save<T>(record: T): Promise<Either<Error, void>>;
  updateBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  updateLargeBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  upsertBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
  upsertLargeBulk<T>(records: T[]): Promise<Either<Error | unknown, void>>;
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

  public async saveLargeBulk<T>(records: T[]): Promise<Either<Error, void>> {
    for (const items of chunk(records, 500)) {
      const result = await this.saveBulk(items);
      if (result.isLeft()) {
        return left(result.value);
      }
    }

    return right(undefined);
  }

  public async updateLargeBulk<T>(records: T[]): Promise<Either<Error, void>> {
    for (const items of chunk(records, 500)) {
      const result = await this.updateBulk(items);
      if (result.isLeft()) {
        return left(result.value);
      }
    }

    return right(undefined);
  }

  public async upsertLargeBulk<T>(records: T[]): Promise<Either<Error, void>> {
    for (const items of chunk(records, 500)) {
      const result = await this.upsertBulk(items);
      if (result.isLeft()) {
        return left(result.value);
      }
    }

    return right(undefined);
  }

  public saveBulk<T>(records: T[]): Promise<Either<Error, void>> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (!isNil(error)) {
          return resolve(left(error as Error));
        }

        records.forEach((item) => {
          transaction.save({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!isNil(error)) {
            return resolve(left(error as Error));
          }
          resolve(right(undefined));
        });
      });
    });
  }

  public async save<T>(record: T): Promise<Either<Error, void>> {
    try {
      const key = this.instance.key(this.collectionName);
      await this.instance.save({
        key,
        properties: record,
      });

      return right(undefined);
    } catch (error) {
      return left(error as Error);
    }
  }

  public updateBulk<T>(records: T[]): Promise<Either<Error, void>> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (!isNil(error)) {
          return resolve(left(error as Error));
        }

        records.forEach((item) => {
          transaction.update({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!isNil(error)) {
            return resolve(left(error as Error));
          }
          resolve(right(undefined));
        });
      });
    });
  }

  public upsertBulk<T>(records: T[]): Promise<Either<Error, void>> {
    const key = this.instance.key(this.collectionName);

    return new Promise((resolve) => {
      const transaction = this.instance.transaction();
      transaction.run((error: unknown | null) => {
        if (!isNil(error)) {
          return resolve(left(error as Error));
        }

        records.forEach((item) => {
          transaction.upsert({ key, data: item });
        });

        transaction.commit((error: unknown | null) => {
          if (!isNil(error)) {
            return resolve(left(error as Error));
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
