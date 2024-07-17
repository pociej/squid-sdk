import { FinalDatabase, FinalTxInfo } from "@subsquid/util-internal-processor-tools";
import { ITopicConfig, Transaction, KafkaConfig } from "kafkajs";
import { ZodSchema } from "zod";
type Topic = {
    dataSchema: ZodSchema;
    topicConfig: ITopicConfig;
};
type ExtractTopic<T> = T extends {
    topicConfig: {
        topic: infer R;
    };
} ? R : never;
type ExtractData<T> = T extends {
    dataSchema: ZodSchema<infer R>;
} ? R : never;
export type Store<T extends Topic[]> = {
    [K in ExtractTopic<T[number]>]: {
        send: (data: ExtractData<T[number]>) => Promise<void>;
    };
};
export declare class KafkaSink<T extends Topic[]> implements FinalDatabase<Store<T>> {
    private topics;
    private config;
    private kafka;
    private producer;
    private isConnected;
    private statusDB;
    constructor(topics: T, config: KafkaConfig);
    connect(): Promise<import("@subsquid/typeorm-store/lib/interfaces").DatabaseState>;
    disconnect(): Promise<void>;
    send(transaction: Transaction, data: any, topic: string): Promise<void>;
    transact(info: FinalTxInfo, cb: (store: Store<T>) => Promise<void>): Promise<void>;
    runInTransaction: (transactionFn: (transactionalStore: Store<T>) => Promise<void>) => Promise<void>;
    private createTransactionStore;
}
export {};
//# sourceMappingURL=store.d.ts.map