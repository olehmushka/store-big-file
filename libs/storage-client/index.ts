import { Storage } from '@google-cloud/storage';
import { Readable, Writable } from 'stream';

export interface IStorageClient {
  createFileReadStream(filename: string): Readable;
  createFileWriteStream(filename: string): Writable;
  deleteFile(filename: string): Promise<void>;
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

  public async deleteFile(filename: string): Promise<void> {
    await this.instance.bucket(this.bucketName).file(filename).delete();
  }
}
