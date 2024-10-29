"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BaseRunner = void 0;
const redis_1 = require("redis");
const node_fs_1 = require("node:fs");
const utils_1 = require("./utils");
const TEN_MINUTES = 10 * 60;
class BaseRunner {
    constructor(redisUrl, config) {
        this.isMaster = false;
        this.totalTestCount = 0;
        this.client = (0, redis_1.createClient)({ url: redisUrl, scripts: this.createRedisScripts() });
        this.config = config;
    }
    async connect() {
        try {
            await this.client.connect();
        }
        catch (e) {
            console.error('[ci-queue] Worker failed to connect');
            throw e;
        }
    }
    async disconnect() {
        await this.client.disconnect();
    }
    async isExhausted() {
        return (await this.isInitialized()) && (await this.size()) === 0;
    }
    async isExpired() {
        const createdAt = await this.client.get(this.key('created-at'));
        return createdAt ? Number(createdAt) + this.config.redisTTL + TEN_MINUTES < (Date.now() / 1000) : true;
    }
    async maxTestsFailed() {
        if (!this.config.maxTestsAllowedToFail) {
            return false;
        }
        const testFailedCount = Number(await this.testFailedCount());
        return testFailedCount >= this.config.maxTestsAllowedToFail;
    }
    async testFailedCount() {
        return await this.client.get(this.key('test_failed_count'));
    }
    async recordFailedTest(testName, testSuite) {
        const fullTestName = `${testName}:${testSuite}`;
        const payload = JSON.stringify({ test_name: testName, test_suite: testSuite });
        await this.client.hSet(this.key('error-reports'), Buffer.from(fullTestName).toString('binary'), Buffer.from(payload).toString('binary'));
        await this.client.expire(this.key('error-reports'), this.config.redisTTL);
        console.log(`[ci-queue] Incrementing failed test count for ${testName}`);
        await this.client.incr(this.key('test_failed_count'));
    }
    async recordPassingTest(testName, testSuite) {
        const fullTestName = `${testName}:${testSuite}`;
        await this.client.hDel(this.key('error-reports'), Buffer.from(fullTestName).toString('binary'));
    }
    async getFailedTests() {
        const failedTests = await this.client.hGetAll(this.key('error-reports'));
        const failures = Object.values(failedTests).map(test => JSON.parse(test));
        return JSON.stringify(failures);
    }
    async waitForMaster() {
        if (this.isMaster) {
            return;
        }
        for (let i = 0; i < this.config.timeout * 10 + 1; i++) {
            if (await this.isInitialized()) {
                return;
            }
            else {
                await (0, utils_1.sleep)(100);
            }
        }
        throw new Error(`The master is still ${await this.getMasterStatus()} after ${this.config.timeout} seconds`);
    }
    async getMasterStatus() {
        const masterStatus = await this.client.get(this.key('master-status'));
        return masterStatus;
    }
    async isInitialized() {
        if (this.queueInitialized) {
            return this.queueInitialized;
        }
        const masterStatus = await this.getMasterStatus();
        const initialized = masterStatus === 'ready' || masterStatus === 'finished';
        if (initialized) {
            this.queueInitialized = true;
        }
        return initialized;
    }
    async size() {
        const [queued, running] = await this.client
            .multi()
            .lLen(this.key('queue'))
            .zCard(this.key('running'))
            .exec();
        return Number(queued) + Number(running);
    }
    async progress() {
        const size = await this.size();
        return this.totalTestCount - size;
    }
    async toArray() {
        return (await this.client
            .multi()
            .lRange(this.key('queue'), 0, -1)
            .zRange(this.key('running'), 0, -1)
            .exec())
            .flatMap((t) => t)
            .reverse();
    }
    key(...args) {
        if (!Array.isArray(args)) {
            args = [args];
        }
        const uniqueID = this.config.namespace ? `${this.config.namespace}:#${this.config.buildId}` : this.config.buildId;
        return ['build', uniqueID, ...args].join(':');
    }
    createRedisScripts() {
        return {
            acknowledge: (0, redis_1.defineScript)({
                NUMBER_OF_KEYS: 3,
                SCRIPT: (0, node_fs_1.readFileSync)(`${__dirname}/../../../redis/acknowledge.lua`).toString(),
                transformArguments(setKey, processedKey, ownersKey, testName) {
                    return [setKey, processedKey, ownersKey, testName];
                },
            }),
            requeue: (0, redis_1.defineScript)({
                NUMBER_OF_KEYS: 6,
                SCRIPT: (0, node_fs_1.readFileSync)(`${__dirname}/../../../redis/requeue.lua`).toString(),
                transformArguments(processedKey, requeuesCountKey, queueKey, setKey, workerQueueKey, ownersKey, maxRequeues, globalMaxRequeues, testName, offset) {
                    return [
                        processedKey,
                        requeuesCountKey,
                        queueKey,
                        setKey,
                        workerQueueKey,
                        ownersKey,
                        maxRequeues.toString(),
                        globalMaxRequeues.toString(),
                        testName,
                        offset.toString(),
                    ];
                },
            }),
            release: (0, redis_1.defineScript)({
                NUMBER_OF_KEYS: 3,
                SCRIPT: (0, node_fs_1.readFileSync)(`${__dirname}/../../../redis/release.lua`).toString(),
                transformArguments(setKey, workerQueueKey, ownersKey) {
                    return [setKey, workerQueueKey, ownersKey];
                },
            }),
            reserve: (0, redis_1.defineScript)({
                NUMBER_OF_KEYS: 5,
                SCRIPT: (0, node_fs_1.readFileSync)(`${__dirname}/../../../redis/reserve.lua`).toString(),
                transformArguments(queueKey, setKey, processedKey, workerQueueKey, ownersKey, currentTime) {
                    return [
                        queueKey,
                        setKey,
                        processedKey,
                        workerQueueKey,
                        ownersKey,
                        currentTime.toString(),
                    ];
                },
                transformReply(reply) {
                    return reply;
                },
            }),
            reserveLost: (0, redis_1.defineScript)({
                NUMBER_OF_KEYS: 4,
                SCRIPT: (0, node_fs_1.readFileSync)(`${__dirname}/../../../redis/reserve_lost.lua`).toString(),
                transformArguments(setKey, completedKey, workerQueueKey, ownersKey, currentTime, timeout) {
                    return [
                        setKey,
                        completedKey,
                        workerQueueKey,
                        ownersKey,
                        currentTime.toString(),
                        timeout.toString(),
                    ];
                },
                transformReply(reply) {
                    return reply;
                },
            }),
        };
    }
}
exports.BaseRunner = BaseRunner;
