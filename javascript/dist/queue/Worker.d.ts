import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
import { TestSpec } from './Test';
export declare class Worker extends BaseRunner {
    private shutdownRequired;
    private currentlyReservedTest;
    constructor(redisUrl: string, config: Configuration);
    pollIter(): AsyncGenerator<string, void, unknown>;
    poll(): Promise<string | null | undefined>;
    acknowledge(test: string): Promise<boolean>;
    requeue(test: string, offset?: number): Promise<boolean>;
    release(): Promise<void>;
    populate(tests: TestSpec[], seed?: number): Promise<boolean>;
    shutdown(): void;
    private throwOnMismatchingTest;
    private reserveTest;
    private tryToReserveTest;
    private tryToReserveLostTest;
    private push;
    private shouldShutdownEarly;
}
