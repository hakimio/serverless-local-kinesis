"use strict";
const tslib_1 = require("tslib");
const aws_sdk_1 = require("aws-sdk");
// @ts-ignore
const kinesalite_1 = tslib_1.__importDefault(require("kinesalite"));
const lodash_1 = require("lodash");
const path = tslib_1.__importStar(require("path"));
const webpackFolder = '.webpack';
class ServerlessLocalKinesis {
    constructor(serverless, options) {
        this.pollKinesis = (functions) => (firstShardIterator) => {
            const mapKinesisRecord = (record) => ({
                approximateArrivalTimestamp: record.ApproximateArrivalTimestamp,
                data: record.Data.toString('base64'),
                partitionKey: record.PartitionKey,
                sequenceNumber: record.SequenceNumber,
            });
            const reduceRecord = (handlers) => (promise, kinesisRecord) => promise.then(() => {
                const singleRecordEvent = { Records: [{ kinesis: mapKinesisRecord(kinesisRecord) }] };
                handlers.forEach(async (handler) => {
                    this.serverlessLog(`🤗 Invoking lambda '${handler}'`);
                    const moduleFileName = `${handler.split('.')[0]}.js`;
                    const handlerFilePath = path.join(this.serverless.config.servicePath, webpackFolder, 'service', moduleFileName);
                    const module = require(handlerFilePath);
                    const functionObjectPath = handler.split('.').slice(1);
                    let mod = module;
                    for (const p of functionObjectPath) {
                        mod = mod[p];
                    }
                    return mod(singleRecordEvent);
                });
            });
            const fetchAndProcessRecords = async (shardIterator) => {
                const records = await this.kinesis.getRecords({ ShardIterator: shardIterator }).promise();
                await records.Records.reduce(reduceRecord(functions), Promise.resolve());
                setTimeout(async () => {
                    await fetchAndProcessRecords(records.NextShardIterator);
                }, 1000);
            };
            return fetchAndProcessRecords(firstShardIterator);
        };
        this.serverless = serverless;
        this.serverlessLog = serverless.cli.log.bind(serverless.cli);
        this.kinesis = new aws_sdk_1.Kinesis({
            endpoint: 'http://localhost:4567',
            region: 'us-east-1',
        });
        this.hooks = {
            'before:offline:start': this.run.bind(this),
            'before:offline:start:init': this.run.bind(this),
        };
    }
    async run() {
        const port = this.serverless.service.custom.kinesis.port || 4567;
        const streamName = this.serverless.service.custom.kinesis.streamName || '';
        const shards = this.serverless.service.custom.kinesis.shards || 1;
        if (streamName === '') {
            throw new Error('No stream name is given');
        }
        try {
            await this.createKinesis(port);
            await this.createStream(streamName, shards);
            await this.watchEvents(streamName);
        }
        catch (e) {
            this.serverlessLog(e);
        }
    }
    createStream(streamName, shards) {
        return new Promise(async (resolve, reject) => {
            try {
                await this.kinesis.createStream({ StreamName: streamName, ShardCount: shards }).promise();
                setTimeout(async () => {
                    const stream = await this.kinesis.describeStream({ StreamName: streamName }).promise();
                    // tslint:disable-next-line:max-line-length
                    this.serverlessLog(`🎊 Stream '${stream.StreamDescription.StreamName}' created with ${stream.StreamDescription.Shards.length} shard(s)`);
                    resolve();
                }, 1000);
            }
            catch (e) {
                reject(e);
            }
        });
    }
    createKinesis(port) {
        const server = new kinesalite_1.default();
        return new Promise((resolve, reject) => {
            server.listen(port, (error) => {
                if (error) {
                    reject(error);
                }
                this.serverlessLog(`🚀 Local kinesis is running at ${port}`);
                resolve();
            });
        });
    }
    async watchEvents(streamName) {
        const stream = await this.kinesis.describeStream({ StreamName: streamName }).promise();
        const { ShardId } = stream.StreamDescription.Shards[0];
        const params = { StreamName: streamName, ShardId, ShardIteratorType: 'LATEST' };
        const shardIterator = await this.kinesis.getShardIterator(params).promise();
        const functions = [];
        for (const name of lodash_1.keys(this.serverless.service.functions)) {
            const serverlessFunction = this.serverless.service.functions[name];
            const streamEvent = lodash_1.filter(serverlessFunction.events || [], (e) => 'stream' in e);
            if (Array.isArray(streamEvent) && streamEvent.length > 0) {
                functions.push(serverlessFunction.handler);
            }
        }
        // added two spaces after the emoji so the log looks fine
        this.serverlessLog('⏰  Polling for events');
        this.pollKinesis(functions)(shardIterator.ShardIterator);
    }
}
module.exports = ServerlessLocalKinesis;
