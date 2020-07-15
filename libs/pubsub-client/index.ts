import { PubSub } from '@google-cloud/pubsub';

export interface IPubSubClient {
  publish(payload: string): Promise<string>;
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

  public publish(payload: string): Promise<string> {
    return this.instance.topic(this.topicName).publish(Buffer.from(payload));
  }
}
