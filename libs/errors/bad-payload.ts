import { CustomError, IError } from './abstract-error';

export interface IBadPubSubPayloadData extends IError {
  subscriptionName: string;
  messageId: string;
}

export class BadPubSubPayload extends CustomError {
  public subscriptionName: string;
  public messageId: string;

  constructor({ message = 'Bad Pub/Sub Payload', subscriptionName, messageId }: IBadPubSubPayloadData) {
    super({ message });
    this.subscriptionName = subscriptionName;
    this.messageId = messageId;
  }
}
