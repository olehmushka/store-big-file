import { CustomError, IError } from './abstract-error';

export class BadPubSubPayload extends CustomError {
  constructor({ message = 'Bad Pub/Sub Payload' }: IError) {
    super({ message });
  }
}
