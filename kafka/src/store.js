"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaSink = void 0;
const kafkajs_1 = require("kafkajs");
const config_1 = require("./config");
const assert_1 = __importDefault(require("assert"));
const typeorm_store_1 = require("@subsquid/typeorm-store");
var MESSAGES;
(function (MESSAGES) {
    MESSAGES["producerAlreadyConnected"] = "Producer already connected";
    MESSAGES["producerNotConnected"] = "Producer not connected";
    MESSAGES["statusDBNotConnected"] = "Status database not connected";
})(MESSAGES || (MESSAGES = {}));
class KafkaSink {
    constructor(topics) {
        this.topics = topics;
        this.runInTransaction = async (transactionFn) => {
            const transaction = await this.createTransactionStore(this.topics);
            try {
                await transactionFn(transaction);
                await transaction.commit();
            }
            catch (error) {
                await transaction.abort();
                throw error;
            }
            finally {
                await this.producer.disconnect();
            }
        };
        //TODO : kafka config should be passed as an argument to the constructor
        this.kafka = new kafkajs_1.Kafka(config_1.KAFKA_CONFIG);
        this.producer = this.kafka.producer();
        this.isConnected = false;
        //for metadata we use postgres
        this.statusDB = new typeorm_store_1.TypeormDatabase();
    }
    async connect() {
        (0, assert_1.default)(!this.isConnected, MESSAGES.producerAlreadyConnected);
        await this.producer.connect();
        this.isConnected = true;
        return await this.statusDB.connect();
    }
    async disconnect() {
        (0, assert_1.default)(this.isConnected, MESSAGES.producerNotConnected);
        await this.producer.disconnect();
        await this.statusDB.disconnect();
        this.isConnected = false;
    }
    async send(transaction, data, topic) {
        (0, assert_1.default)(this.isConnected, MESSAGES.producerNotConnected);
        await transaction.send({
            topic,
            messages: [{ value: JSON.stringify(data) }],
        });
    }
    async transact(info, cb) {
        (0, assert_1.default)(this.isConnected, MESSAGES.producerNotConnected);
        //TODO : Implement the transaction logic, probably 2PC will be
        //NOTE: empty callback is passed to the transact method of the statusDB
        //we want only status update but it is private to the TypeormDatabase class
        this.statusDB.transact(info, (_store) => {
            return Promise.resolve();
        });
        return this.runInTransaction(cb);
    }
    async createTransactionStore(topics) {
        const transaction = await this.producer.transaction();
        const store = topics
            .map((topic) => {
            const topicName = topic.topicConfig.topic;
            return {
                [topicName]: {
                    send: async (data) => {
                        topic.dataSchema.parse(data);
                        await this.send(transaction, topic, data);
                    },
                },
            };
        })
            .reduce((acc, curr) => ({ ...acc, ...curr }), {});
        return { ...transaction, ...store };
    }
}
exports.KafkaSink = KafkaSink;
