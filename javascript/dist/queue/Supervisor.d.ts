import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
export declare class Supervisor extends BaseRunner {
    constructor(redisUrl: string, config: Configuration);
    totalTests(): Promise<string | null>;
    waitForWorkers(): Promise<boolean>;
    private workersAreActive;
}
