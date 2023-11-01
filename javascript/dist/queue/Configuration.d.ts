export declare type InitConfig = {
    buildId: string;
    workerId: string;
    seed?: string;
    redisTTL?: number;
    maxRequeues?: number;
    requeueTolerance?: number;
    maxTestsAllowedToFail?: number;
    timeout?: number;
    reportTimeout?: number;
    inactiveWorkersTimeout?: number;
};
export declare class Configuration {
    buildId: string;
    workerId: string;
    seed?: string;
    maxRequeues: number;
    requeueTolerance: number;
    maxTestsAllowedToFail: number;
    redisTTL: number;
    timeout: number;
    reportTimeout: number;
    inactiveWorkersTimeout: number;
    constructor({ buildId, workerId, seed, redisTTL, maxRequeues, requeueTolerance, maxTestsAllowedToFail, timeout, reportTimeout, inactiveWorkersTimeout, }: {
        buildId: string;
        workerId: string;
        seed?: string;
        redisTTL?: number;
        maxRequeues?: number;
        requeueTolerance?: number;
        maxTestsAllowedToFail?: number;
        timeout?: number;
        reportTimeout?: number;
        inactiveWorkersTimeout?: number;
    });
    static fromEnv(): any;
    globalMaxRequeues(testCount: number): number;
}
