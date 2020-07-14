const { Storage } = require('@google-cloud/storage');
const csvSplitStream = require('csv-split-stream');

/**
 * Generic background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.splitCsvFunction = (data, context, callback) => {
  if (!data.name.includes('input')) {
    return callback(null, null);
  }
  const storage = new Storage()
  const timestamp = Date.now();
  const outputFilenames = [];

  const readFileStream = storage.bucket(data.bucket).file(data.name).createReadStream();
  csvSplitStream.split(readFileStream, { lineLimit: 100 },
    (index) => {
      const outputFilename = `output-${timestamp}-${index}.csv`;
      outputFilenames.push(outputFilename);

      return storage.bucket(data.bucket).file(outputFilename).createWriteStream();
    }
  )
    .then(() => {
      console.log({ filenames: outputFilenames });
    })
    .then(() => {
      callback(null, 'Finished');
    })
    .catch((error) => {
      callback(error, null);
    });
};
