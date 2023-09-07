import { RedisScripts, createClient, defineScript } from 'redis';
import { readFileSync } from 'node:fs';
import { sleep } from './utils';
import { Configuration } from './Configuration';

const TEN_MINUTES = 10 * 60;

export class BaseRunner {
  isMaster: boolean = false;
  config: Configuration;
  client;
  private queueInitialized?: boolean;
  constructor(redisUrl: string, config: Configuration) {
    this.client = createClient({ url: redisUrl, scripts: this.createRedisScripts() });
    this.config = config;
  }

  async connect() {
    await this.client.connect();
  }

  async queueIsExhausted(): Promise<boolean> {
    return (await this.queueIsInitialized()) && (await this.queueSize()) === 0;
  }

  async isQueueExpired(): Promise<boolean> {
    const createdAt = await this.client.get(this.key('created-at'));
    return createdAt ? Number(createdAt) + this.config.redisTTL + TEN_MINUTES < Date.now() : true;
  }

  async maxTestsFailed(): Promise<boolean> {
    if (!this.config.maxTestsAllowedToFail) {
      return false;
    }
    const testFailedCount = Number(await this.testFailedCount());
    return testFailedCount >= this.config.maxTestsAllowedToFail;
  }

  async testFailedCount() {
    return await this.client.get(this.key('test_failed_count'));
  }

  async incTestFailedCount() {
    await this.client.incr(this.key('test_failed_count'));
  }

  async waitForMaster(): Promise<void> {
    if (this.isMaster) {
      return;
    }
    for (let i = 0; i < this.config.timeout * 10 + 1; i++) {
      if (await this.queueIsInitialized()) {
        return;
      } else {
        await sleep(100);
      }
    }
    throw new Error(
      `The master is still ${await this.getMasterStatus()} after ${this.config.timeout} seconds`,
    );
  }

  async getMasterStatus(): Promise<string> {
    const masterStatus = await this.client.get(this.key('master-status'));
    if (!masterStatus) {
      throw new Error('Master status is null');
    }
    return masterStatus;
  }

  async queueIsInitialized(): Promise<boolean> {
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

  async queueSize() {
    const [queued, running] = await this.client
      .multi()
      .lLen(this.key('queue'))
      .zCard(this.key('running'))
      .exec();
    return Number(queued) + Number(running);
  }

  key(...args: string[]): string {
    if (!Array.isArray(args)) {
      args = [args];
    }
    return ['build', this.config.buildId, ...args].join(':');
  }

  private createRedisScripts() {
    return {
      acknowledge: defineScript({
        NUMBER_OF_KEYS: 3,
        SCRIPT: readFileSync('../../redis/acknowledge.lua').toString(),
        transformArguments(
          setKey: string,
          processedKey: string,
          ownersKey: string,
          testName: string,
        ) {
          return [setKey, processedKey, ownersKey, testName];
        },
      }),
      requeue: defineScript({
        NUMBER_OF_KEYS: 6,
        SCRIPT: readFileSync('../../redis/requeue.lua').toString(),
        transformArguments(
          processedKey: string,
          requeuesCountKey: string,
          queueKey: string,
          setKey: string,
          workerQueueKey: string,
          ownersKey: string,
          maxRequeues: number,
          globalMaxRequeues: number,
          testName: string,
          offset: number,
        ) {
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
      release: defineScript({
        NUMBER_OF_KEYS: 3,
        SCRIPT: readFileSync('../../redis/release.lua').toString(),
        transformArguments(setKey: string, workerQueueKey: string, ownersKey: string) {
          return [setKey, workerQueueKey, ownersKey];
        },
      }),

      reserve: defineScript({
        NUMBER_OF_KEYS: 4,
        SCRIPT: readFileSync('../../redis/reserve.lua').toString(),
        transformArguments(
          queueKey: string,
          setKey: string,
          processedKey: string,
          workerQueueKey: string,
          ownersKey: string,
          currentTime: number,
        ) {
          return [
            queueKey,
            setKey,
            processedKey,
            workerQueueKey,
            ownersKey,
            currentTime.toString(),
          ];
        },
      }),
      reserveLost: defineScript({
        NUMBER_OF_KEYS: 4,
        SCRIPT: readFileSync('../../redis/reserve_lost.lua').toString(),
        transformArguments(
          setKey: string,
          completedKey: string,
          workerQueueKey: string,
          ownersKey: string,
          currentTime: number,
          timeout: number,
        ) {
          return [
            setKey,
            completedKey,
            workerQueueKey,
            ownersKey,
            currentTime.toString(),
            timeout.toString(),
          ];
        },
      }),
    };
  }
}
