import { Transform } from 'stream';
import { WriteBatch } from '@google-cloud/firestore';
import { IFirestoreClient } from '../firestore-client';
import { IUser } from '../contracts';

export type CustomWriteBatch = WriteBatch & { _ops: { length: number } };

export interface IUploadStreamOptions {
  objectMode?: boolean;
  batchSize: number;
  parallelCommitSize: number;
  loadDate: string;
}

export class UploadStream extends Transform {
  private batchSize = 100;
  private batch: IUser[] = [];
  private parallelCommitSize = 1;
  private parallelBatchesForCommit: CustomWriteBatch[] = []
  private storedNumberOfRecords = 0;
  private loadDate: string;

  constructor(
    private firestoreClient: IFirestoreClient,
    options: IUploadStreamOptions,
  ) {
    super({ ...options, objectMode: true });

    if (options.batchSize) {
      this.batchSize = options.batchSize;
    }

    if (options.parallelCommitSize) {
      this.parallelCommitSize = options.parallelCommitSize;
    }
    this.loadDate = options.loadDate;
  }

  public _transform(record: IUser, encoding: any, callback: Function): void {
    this.batch.push(record);
    if (this.shouldPushToParallelBatches) {
      this.storeParallelBatch();
    }

    if (this.shouldSaveBatches) {
      // we have hit our batch size to process the records as a batch
      this.saveBatches()
        // we successfully processed the records so callback
        .then(() => callback())
        // An error occurred!
        .catch(err => callback(err));

      return;
    }
    // we shouldn't persist so ignore
    callback();
  }

  public _flush(callback: Function): void {
    if (this.batch.length) {
      this.storeParallelBatch();
    }

    if (this.parallelBatchesForCommit.length) {
      // we have hit our batch size to process the records as a batch
      this.saveBatches()
        // we successfully processed the records so callback
        .then(() => callback())
        // An error occurred!
        .catch(err => callback(err));

      return;
    }
    // no records to persist so just call callback
    callback();
  }

  public pushRecordsToDownStream(records: IUser[]): void {
    // emit each record for down stream processing
    records.forEach(r => this.push(r));
  }

  get shouldSaveBatches(): boolean {
    return this.parallelBatchesForCommit.length >= this.parallelCommitSize
  }

  get shouldPushToParallelBatches(): boolean {
    return this.batch.length >= this.batchSize;
  }

  private storeParallelBatch(): void {
    const records = this.batch;
    const batchCommit = this.createCommitBatchToFirestore(records);
    this.parallelBatchesForCommit.push(batchCommit);
    this.batch = [];
    // be sure to emit them
    this.pushRecordsToDownStream(records);
  }

  // Should be < 500
  private createCommitBatchToFirestore(recordsBatch: IUser[]): CustomWriteBatch {
    const batch = this.firestoreClient.instance.batch();

    recordsBatch.forEach((record) => {
      const docRef = this.firestoreClient.instance
        .collection(this.firestoreClient.collectionName)
        .doc(record.email);
      const timeMarkedRecord = {
        email: record.email,
        eligible: Boolean(record.eligible),
        loadDate: this.loadDate
      }
      batch.set(
        docRef,
        timeMarkedRecord,
        // { merge: true }
      )
    });

    return batch as CustomWriteBatch;
  }

  private async saveBatches(): Promise<void> {
    const batchesForCommit = this.parallelBatchesForCommit;
    // This is where you should save/update/delete the records
    await this.commitBatchesToFirestore(batchesForCommit);
    this.parallelBatchesForCommit = [];
  }

  private async commitBatchesToFirestore(batchesForCommit: CustomWriteBatch[]): Promise<void> {
    let batchSizeFromResponse = 0;

    return Promise.all(batchesForCommit.map((batchForCommit) => {
      batchSizeFromResponse += batchForCommit?._ops?.length ?? 0;

      return batchForCommit.commit();
    }))
      .then(() => {
        this.storedNumberOfRecords += batchSizeFromResponse;
        const batchLength = batchesForCommit.length;
        const { storedNumberOfRecords } = this;
        console.log(`All ${batchLength} batches committed by Promise.all. STORED items: ${storedNumberOfRecords}`);
      })
      .catch(e => {
        console.error(`Batch storing failed: ${e}`)
      });
  }
}
