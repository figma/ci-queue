"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Worker = void 0;
const BaseRunner_1 = require("./BaseRunner");
const utils_1 = require("./utils");
class Worker extends BaseRunner_1.BaseRunner {
    constructor(redisUrl, config) {
        super(redisUrl, config);
        this.shutdownRequired = false;
    }
    async *pollIter() {
        let lastLogTime = 0;
        await this.waitForMaster();
        while (!this.shutdownRequired &&
            !(await this.isExhausted()) &&
            !(await this.maxTestsFailed())) {
            const test = await this.reserveTest();
            if (test) {
                console.log(`[ci-queue] Reserved test: ${test}`);
                yield test;
            }
            else {
                const shouldShutdownEarly = await this.shouldShutdownEarly();
                if (shouldShutdownEarly) {
                    console.log(`[ci-queue] No more tests to pick up for now, will shutdown early`);
                    break;
                }
                const now = Date.now() / 1000;
                // Only log every 5s to reduce noise
                if (now - lastLogTime > 5) {
                    console.log('[ci-queue] No test to reserve, sleeping');
                    lastLogTime = now;
                }
                await (0, utils_1.sleep)(500);
            }
        }
        await Promise.all([
            this.client.expire(this.key('worker', this.config.workerId, 'queue'), this.config.redisTTL),
            this.client.expire(this.key('processed'), this.config.redisTTL),
        ]);
    }
    async poll() {
        await this.waitForMaster();
        if (this.shutdownRequired || (await this.isExhausted()) || (await this.maxTestsFailed())) {
            return;
        }
        return await this.reserveTest();
    }
    async acknowledge(test) {
        this.throwOnMismatchingTest(test);
        return ((await this.client.acknowledge(this.key('running'), this.key('processed'), this.key('owners'), test)) === 1);
    }
    async requeue(test, offset = 42) {
        const testName = test;
        this.throwOnMismatchingTest(testName);
        const globalMaxRequeues = this.config.globalMaxRequeues(this.totalTestCount);
        const requeued = this.config.maxRequeues > 0 &&
            globalMaxRequeues > 0 &&
            (await this.client.requeue(this.key('processed'), this.key('requeues-count'), this.key('queue'), this.key('running'), this.key('worker', this.config.workerId, 'queue'), this.key('owners'), this.config.maxRequeues, globalMaxRequeues, testName, offset)) === 1;
        if (!requeued) {
            this.currentlyReservedTest = testName;
        }
        return requeued;
    }
    async release() {
        await this.client.release(this.key('running'), this.key('worker', this.config.workerId, 'queue'), this.key('owners'));
    }
    async populate(tests, seed) {
        if (this.config.retriedBuildId) {
            console.log(`[ci-queue] Retrying failed tests for build ${this.config.retriedBuildId}`);
            const failedTestGroups = await this.getFailedTestGroupsFromPreviousBuild();
            if (failedTestGroups.length === 0) {
                console.log(`[ci-queue] No failed tests found for build ${this.config.retriedBuildId}`);
                return false;
            }
            console.log(`[ci-queue] Failed test groups: ${failedTestGroups}`);
            await this.push(failedTestGroups);
            return true;
        }
        console.log(`[ci-queue] Populating tests`);
        if (seed !== undefined) {
            tests = (0, utils_1.shuffleArray)(tests, seed);
        }
        await this.push(tests);
        return true;
    }
    shutdown() {
        this.shutdownRequired = true;
    }
    throwOnMismatchingTest(testName) {
        if (this.currentlyReservedTest === testName) {
            this.currentlyReservedTest = undefined;
        }
        else {
            throw new Error(`Acknowledged ${testName} but ${this.currentlyReservedTest} was reserved`);
        }
    }
    async reserveTest() {
        if (this.currentlyReservedTest) {
            console.error(`[ci-queue] Currently reserved test found for worker ${this.config.workerId}:`, this.currentlyReservedTest);
            throw new Error(`${this.currentlyReservedTest} is already reserved. You have to acknowledge it before you can reserve another one`);
        }
        const reservedTest = (await this.tryToReserveLostTest()) ?? (await this.tryToReserveTest());
        this.currentlyReservedTest = reservedTest;
        return reservedTest;
    }
    async tryToReserveTest() {
        return await this.client.reserve(this.key('queue'), this.key('running'), this.key('processed'), this.key('worker', this.config.workerId, 'queue'), this.key('owners'), Date.now() / 1000);
    }
    async tryToReserveLostTest() {
        const lostTest = await this.client.reserveLost(this.key('running'), this.key('completed'), this.key('worker', this.config.workerId, 'queue'), this.key('owners'), Date.now() / 1000, this.config.timeout);
        return lostTest;
    }
    async push(tests) {
        this.totalTestCount = tests.length;
        this.isMaster = await this.client.setNX(this.key('master-status'), 'setup');
        if (this.isMaster) {
            await this.client
                .multi()
                .lPush(this.key('queue'), tests)
                .set(this.key('total'), this.totalTestCount)
                .set(this.key('master-status'), 'ready')
                .expire(this.key('queue'), this.config.redisTTL)
                .expire(this.key('total'), this.config.redisTTL)
                .expire(this.key('master-status'), this.config.redisTTL)
                .exec();
        }
        await this.client.sAdd(this.key('workers'), [this.config.workerId]);
        await this.client.expire(this.key('workers'), this.config.redisTTL);
    }
    async shouldShutdownEarly() {
        const totalWorkers = await this.client.sMembers(this.key('workers'));
        const totalWorkersCount = totalWorkers.length;
        // Only keep 20% of workers running
        const stayRunningThreshold = Math.max(Math.floor(totalWorkersCount * 0.2), 1);
        const parallelJob = parseInt(this.config.workerId, 10);
        const shouldShutdownEarly = parallelJob > stayRunningThreshold;
        return shouldShutdownEarly;
    }
}
exports.Worker = Worker;
