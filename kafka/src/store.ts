import {
  FinalDatabase,
  FinalTxInfo,
} from "@subsquid/util-internal-processor-tools";
import { Kafka, Producer, ITopicConfig } from "kafkajs";
import { KAFKA_CONFIG } from "./config";
import assert from "assert";
import { z, ZodSchema } from "zod";

const PRODUCER_ALREADY_CONNECTED_MESSAGE = "Producer already connected";

//For now we are only getting data from the lake
//TODO : Add support for hot blocks

type Topic = {
  dataSchema: ZodSchema<any>;
  topicConfig: ITopicConfig;
};

type Store<T extends Topic[]> = Readonly<{
  [k in keyof T]: {
    send: (data: z.infer<T[k]["dataSchema"]>) => Promise<void>;
  };
}>;

export class KafkaSink<T extends Topic[]> implements FinalDatabase<Store<T>> {
  private kafka: Kafka;
  private producer: Producer;
  private connection: any;

  constructor(private topics: Topic) {
    //TODO : kafka config should be passed as an argument to the constructor
    this.kafka = new Kafka(KAFKA_CONFIG);
    this.producer = this.kafka.producer();
    this.connection = null;
  }

  //@ts-ignore
  async connect() {
    throw new Error("Method not implemented.");
  }

  async disconnect() {
    assert(this.connection !== null, "Producer not connected");
    await this.producer.disconnect();
    this.connection = null;
  }

  async send(data: any, topic: string) {
    assert(this.connection !== null, "Producer not connected");
    await this.producer.send({
      topic,
      messages: [{ value: JSON.stringify(data) }],
    });
  }

  transact(
    info: FinalTxInfo,
    cb: (store: Store<T>) => Promise<void>,
  ): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async initTopics(topics: ITopicConfig[]) {
    const admin = this.kafka.admin();
    await admin.connect();
    const clusterInfo = await admin.describeCluster();

    const stateTopic = {
      topic: "state",
      numPartitions: 1,
      replicationFactor: clusterInfo.brokers.length,
    };

    await admin.createTopics({
      topics: [stateTopic, ...topics],
    });
    await admin.disconnect();
  }
}
