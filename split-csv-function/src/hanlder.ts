import { IStorageClient } from '../../libs/storage-client';
import { IPubSubClient } from '../../libs/pubsub-client';

const csvSplitStream = require('csv-split-stream');

export class SplitCsvFile {
  constructor(private storageClient: IStorageClient, private pubSubClient: IPubSubClient) {}

  public async handle(filename: string): Promise<string[]> {
    const timestamp = Date.now();
    const outputFilenames: string[] = [];
    const readFileStream = this.storageClient.createFileReadStream(filename);
    await csvSplitStream.split(readFileStream, { lineLimit: 100 }, (index: number) => {
      const outputFilename = `output-${timestamp}-${index}.csv`;
      outputFilenames.push(outputFilename);

      return this.storageClient.createFileWriteStream(outputFilename);
    });
    const publishedMessageIDs = await Promise.all(
      outputFilenames.map((filename) => this.pubSubClient.publish(JSON.stringify({ data: { csvFilename: filename } }))),
    );

    return publishedMessageIDs;
  }
}
