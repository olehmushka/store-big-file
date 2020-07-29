import { Storage } from '@google-cloud/storage';
import { Either, right, left } from '@sweet-monads/either';
import { Readable, Writable } from 'stream';

export interface IStorageClient {
  createFileReadStream(filename: string): Readable;
  createFileWriteStream(filename: string): Writable;
  deleteFile(filename: string): Promise<Either<Error, void>>;
  moveFileToBucket(distBucketName: string, filename: string): Promise<Either<Error, void>>;
  renameFile(oldFilename: string, newFilename: string): Promise<Either<Error, void>>;
}

export interface IStorageClientConfig {
  bucketName: string;
}

export class StorageClient implements IStorageClient {
  private readonly instance: Storage
  private bucketName: string;

  constructor(instance: Storage, storageConfig: IStorageClientConfig) {
    this.instance = instance;
    this.bucketName = storageConfig.bucketName;
  }

  public createFileReadStream(filename: string): Readable {
    return this.instance.bucket(this.bucketName).file(filename).createReadStream();
  }

  public createFileWriteStream(filename: string): Writable {
    return this.instance.bucket(this.bucketName).file(filename).createWriteStream();
  }

  public async deleteFile(filename: string): Promise<Either<Error, void>> {
    try {
      await this.instance.bucket(this.bucketName).file(filename).delete();

      return right(undefined);
    } catch (error) {
      return left(error);
    }
  }

  public async moveFileToBucket(distBucketName: string, filename: string): Promise<Either<Error, void>> {
    try {
      await this.instance.bucket(this.bucketName)
        .file(filename)
        .copy(this.instance.bucket(distBucketName).file(filename));
      await this.deleteFile(filename);

      return right(undefined);
    } catch (error) {
      return left(error as Error);
    }
  }

  public async renameFile(oldFilename: string, newFilename: string): Promise<Either<Error, void>> {
    try {
      await this.instance.bucket(this.bucketName).file(oldFilename).move(newFilename);

      return right(undefined);
    } catch (error) {
      return left(error as Error);
    }
  }
}

export const getStorageClient = (
  instance: Storage,
  storageConfig: IStorageClientConfig,
): IStorageClient => new StorageClient(instance, storageConfig);
