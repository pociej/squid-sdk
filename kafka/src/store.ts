import {
  FinalDatabase,
  FinalTxInfo,
} from "@subsquid/util-internal-processor-tools";
import {
  Kafka,
  Producer,
  ITopicConfig,
  Transaction,
  KafkaConfig,
} from "kafkajs";
import assert from "assert";
import { z, ZodSchema } from "zod";
import { TypeormDatabase } from "@subsquid/typeorm-store";

enum MESSAGES {
  producerAlreadyConnected = "Producer already connected",
  producerNotConnected = "Producer not connected",
  statusDBNotConnected = "Status database not connected",
}

//For now we are only getting data from the lake
//TODO : Add support for hot blocks
type Topic = {
  dataSchema: ZodSchema;
  topicConfig: ITopicConfig;
};

type ExtractTopic<T> = T extends {
  topicConfig: { topic: infer R };
}
  ? R
  : never;

type ExtractData<T> = T extends { dataSchema: ZodSchema<infer R> } ? R : never;

export type Store<T extends Topic[]> = {
  [K in ExtractTopic<T[number]>]: {
    send: (data: ExtractData<T[number]>) => Promise<void>;
  };
};

const bigIntReplacer = (key: string, value: unknown) => {
  if (typeof value === "bigint") {
    return value.toString();
  }
  return value;
};

export class KafkaSink<T extends Topic[]> implements FinalDatabase<Store<T>> {
  private kafka: Kafka;
  private producer: Producer;
  private isConnected: boolean;
  private statusDB: TypeormDatabase;
  constructor(
    private topics: T,
    private config: KafkaConfig,
  ) {
    this.kafka = new Kafka(config);
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      transactionalId: "squid-processor",
    });

    this.isConnected = false;
    //for metadata we use postgres
    this.statusDB = new TypeormDatabase();
  }

  async connect() {
    assert(!this.isConnected, MESSAGES.producerAlreadyConnected);
    await this.producer.connect();
    this.isConnected = true;
    return await this.statusDB.connect();
  }

  async disconnect() {
    assert(this.isConnected, MESSAGES.producerNotConnected);
    await this.producer.disconnect();
    await this.statusDB.disconnect();
    this.isConnected = false;
  }

  async send(transaction: Transaction, data: any, topic: string) {
    assert(this.isConnected, MESSAGES.producerNotConnected);
    await transaction.send({
      topic,
      messages: [{ value: JSON.stringify(data, bigIntReplacer) }],
    });
  }

  async transact(
    info: FinalTxInfo,
    cb: (store: Store<T>) => Promise<void>,
  ): Promise<void> {
    assert(this.isConnected, MESSAGES.producerNotConnected);
    //TODO : Implement the transaction logic, probably 2PC will be

    //NOTE: empty callback is passed to the transact method of the statusDB
    //we want only status update but it is private to the TypeormDatabase class

    await this.statusDB.transact(info, (_store) => {
      return Promise.resolve();
    });
    return await this.runInTransaction(cb);
  }

  runInTransaction = async (
    transactionFn: (transactionalStore: Store<T>) => Promise<void>,
  ) => {
    const { transaction, store } = await this.createTransactionStore(
      this.topics,
    );
    try {
      await transactionFn(store);
      await transaction.commit();
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  };

  private async createTransactionStore(topics: Topic[]): Promise<{
    store: Store<T>;
    transaction: Transaction;
  }> {
    const transaction = await this.producer.transaction();
    const store: Store<T> = topics
      .map((topic) => {
        const topicName = topic.topicConfig.topic as keyof Store<T>;

        return {
          [topicName]: {
            send: async (data: z.infer<typeof topic.dataSchema>) => {
              await topic.dataSchema.parse(data);
              await this.send(transaction, data, topicName);
            },
          },
        };
      })
      .reduce((acc, curr) => ({ ...acc, ...curr }), {} as Store<T>);

    return {
      store,
      transaction,
    };
  }
}
