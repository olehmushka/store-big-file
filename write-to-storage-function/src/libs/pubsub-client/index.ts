import { PubSub } from '@google-cloud/pubsub';
import { Either, right, left } from '@sweet-monads/either';

export interface IPubSubClient {
  publish(payload: string): Promise<Either<Error, string>>;
}

export interface IPubSubClientConfig {
  topicName: string;
}

export class PubSubClient implements IPubSubClient {
  private readonly instance: PubSub;
  private topicName: string;

  constructor(instance: PubSub, pubSubConfig: IPubSubClientConfig) {
    this.instance = instance;
    this.topicName = pubSubConfig.topicName;
  }

  public async publish(payload: string): Promise<Either<Error, string>> {
    try {
      const messageId = await this.instance.topic(this.topicName).publish(Buffer.from(payload));

      return right(messageId);
    } catch (error) {
      return left(error);
    }
  }
}
