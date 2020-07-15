const SPLITTED_CSV_FILE_SUBSCRIPTION_NAME = 'splitted-csv-file-subscription';

class CustomError extends Error {
  constructor({ message }) {
    super(message);
  }
}

class BadPubSubPayload extends CustomError {
  constructor({ message = 'Bad Pub/Sub Payload', subscriptionName, messageId } = {}) {
    super({ message });
    this.subscriptionName = subscriptionName;
    this.messageId = messageId;
  }
}

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.storeDataFunction = (pubSubMessage, context, callback) => {
  if (pubSubMessage.data == null) {
    return callback(new BadPubSubPayload({
      message: 'Data is empty',
      subscriptionName: SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }

  let payload = null;
  try {
    payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());
  } catch {
    return callback(new BadPubSubPayload({
      message: 'Can not parse payload',
      subscriptionName: SPLITTED_CSV_FILE_SUBSCRIPTION_NAME,
      messageId: pubSubMessage.messageId,
    }), { success: false });
  }
  console.log({ payload });

  callback(null, { success: true });
};
