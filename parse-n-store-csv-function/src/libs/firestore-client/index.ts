import { Firestore } from '@google-cloud/firestore';
import { Either, right, left } from '@sweet-monads/either';
import { chunk } from '../utils';

export interface IFirestoreClient {
  instance: Firestore;
  collectionName: string;
  set<T extends { [key: string]: any }>(record: T, keyField: string): Promise<Either<Error, void>>;
  setBulk<T extends { [key: string]: any }>(records: T[], keyField: string): Promise<Either<Error, void>>;
  setLargeBulk<T extends { [key: string]: any }>(
    records: T[],
    keyField: string,
  ): Promise<Either<Error | unknown, void>>;
}

export interface IFirestoreClientConfig {
  collectionName: string;
}

export class FirestoreClient implements IFirestoreClient {
  public readonly instance: Firestore;
  public collectionName: string;

  constructor(instance: Firestore, firestoreConfig: IFirestoreClientConfig) {
    this.instance = instance;
    this.collectionName = firestoreConfig.collectionName;
  }

  public async set<T extends { [key: string]: any }>(record: T, keyField: string): Promise<Either<Error, void>> {
    try {
      await this.instance.collection(this.collectionName).doc(record[keyField]).set(record);

      return right(undefined);
    } catch (error) {
      return left(error as Error);
    }
  }

  public async setBulk<T extends { [key: string]: any }>(
    records: T[],
    keyField: string,
  ): Promise<Either<Error, void>> {
    const batch = this.instance.batch();

    for (const record of records) {
      const ref = this.instance.collection(this.collectionName).doc(record[keyField]);
      batch.set(ref, record);
    }

    try {
      await batch.commit();

      return right(undefined);
    } catch (error) {
      return left(error as Error);
    }
  }

  public async setLargeBulk<T extends { [key: string]: any }>(
    records: T[],
    keyField: string,
  ): Promise<Either<Error | unknown, void>> {
    for (const items of chunk(records, 500)) {
      const result = await this.setBulk(items, keyField);
      if (result.isLeft()) {
        return left(result.value);
      }
    }

    return right(undefined);
  }
}
