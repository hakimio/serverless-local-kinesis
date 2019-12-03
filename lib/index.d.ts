declare class ServerlessLocalKinesis {
    private serverless;
    private readonly serverlessLog;
    private kinesis;
    private hooks;
    constructor(serverless: any, options: any);
    pollKinesis: (functions: string[]) => (firstShardIterator: string) => Promise<void>;
    private run;
    private createStream;
    private createKinesis;
    private watchEvents;
}
export = ServerlessLocalKinesis;
