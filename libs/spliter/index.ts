import { Readable, Writable } from 'stream';
import { isNil } from '../utils';

const byline = require('byline');

export interface ISplitOptions {
  delimiter?: string;
  lineLimit: number;
}

export interface ISplitResult {
  totalChunks: number;
  options: ISplitOptions;
}

type OutputStreamCallback = (index: number) => Writable;

export const split = (
  inputStream: Readable,
  opts: ISplitOptions,
  createOutputStreamCallback: OutputStreamCallback,
): Promise<ISplitResult> => {
  let outputStream: Writable | null = null;
  let chunkIndex = 0;
  let lineIndex = 0;
  let header: any;
  const options = {
    delimiter: opts.delimiter || '\n',
    lineLimit: opts.lineLimit
  };

  return new Promise((resolve, reject) => {
    if (isNil(inputStream)) {
      return reject(new Error('Provide inputStream'));
    }

    if (options.lineLimit <= 0) {
      return reject(new Error('Provide non-negative lineLimit'));
    }

    let lineStream;

    function handleError(err: Error): void {
      if (!isNil(outputStream)) {
        outputStream.end();
      }
      reject(err);
    }

    inputStream.on('error', handleError);

    try {
      lineStream = byline(inputStream);
    } catch(err) {
      handleError(err);

      return;
    }

    lineStream.on('data', (line: any) => {
      if (!header) {
        header = line;
      } else {
        if (lineIndex === 0) {
          if (outputStream) {
            outputStream.end();
          }
          outputStream = createOutputStreamCallback(chunkIndex++);
          outputStream.write(header);
          outputStream.write(options.delimiter);
        }

        outputStream?.write(line);
        outputStream?.write(options.delimiter);
        lineIndex = (++lineIndex) % options.lineLimit;
      }
    });

    lineStream.on('error', handleError);
    lineStream.on('end', () => {
      if (!header) {
        reject(new Error('The provided CSV is empty'));

        return;
      }

      if (outputStream) {
        outputStream.end();
      } else {
        outputStream = createOutputStreamCallback(chunkIndex++);
        outputStream.write(header);
        outputStream.write(options.delimiter);
        outputStream.end();
      }

      resolve({
        totalChunks: chunkIndex,
        options: options
      });
    });
  });
}
