export type InitConfig = {
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
    namespace?: string;
    failureFile?: string;
    retriedBuildId?: string;
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
    namespace?: string;
    failureFile?: string;
    retriedBuildId?: string;
    constructor({ buildId, workerId, seed, redisTTL, maxRequeues, requeueTolerance, maxTestsAllowedToFail, timeout, reportTimeout, inactiveWorkersTimeout, namespace, failureFile, retriedBuildId, }: {
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
        namespace?: string;
        failureFile?: string;
        retriedBuildId?: string;
    });
    static fromEnv(): any;
    globalMaxRequeues(testCount: number): number;
}
