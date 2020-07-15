import { CustomError, IError } from './abstract-error';

export class BadPubSubPayload extends CustomError {
  public name = 'Bad Pub/Sub Payload';
  constructor({ message = 'Bad Pub/Sub Payload' }: IError) {
    super({ message });
  }
}

export class CanNotPublishMessage extends CustomError {
  public name = 'Can not publish Pub/Sub message';
  constructor({ message = 'Can not publish Pub/Sub message' }: IError) {
    super({ message });
  }
}
