import { createClient, defineScript } from 'redis';
import { readFileSync } from 'node:fs';
import { sleep } from './utils';
import { Configuration } from './Configuration';

const TEN_MINUTES = 10 * 60;

export class BaseRunner {
  isMaster: boolean = false;
  config: Configuration;
  client;
  totalTestCount: number = 0;
  private queueInitialized?: boolean;
  constructor(redisUrl: string, config: Configuration) {
    this.client = createClient({ url: redisUrl, scripts: this.createRedisScripts() });
    this.config = config;
  }

  async connect() {
    try {
      await this.client.connect();
    } catch (e) {
      console.error('Worker failed to connect');
      throw e;
    }
  }

  async disconnect() {
    await this.client.disconnect();
  }

  async isExhausted(): Promise<boolean> {
    return (await this.isInitialized()) && (await this.size()) === 0;
  }

  async isExpired(): Promise<boolean> {
    const createdAt = await this.client.get(this.key('created-at'));
    return createdAt ? Number(createdAt) + this.config.redisTTL + TEN_MINUTES < (Date.now() / 1000) : true;
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

  async incFailedTestCount() {
    await this.client.incr(this.key('test_failed_count'));
  }

  async waitForMaster(): Promise<void> {
    if (this.isMaster) {
      return;
    }
    for (let i = 0; i < this.config.timeout * 10 + 1; i++) {
      if (await this.isInitialized()) {
        return;
      } else {
        await sleep(100);
      }
    }
    throw new Error(
      `The master is still ${await this.getMasterStatus()} after ${this.config.timeout} seconds`,
    );
  }

  async getMasterStatus(): Promise<string | null> {
    const masterStatus = await this.client.get(this.key('master-status'));
    return masterStatus;
  }

  async isInitialized(): Promise<boolean> {
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

  async toArray(): Promise<string[]> {
    return (
      await this.client
        .multi()
        .lRange(this.key('queue'), 0, -1)
        .zRange(this.key('running'), 0, -1)
        .exec()
    )
      .flatMap((t) => t)
      .reverse() as string[];
  }

  key(...args: string[]): string {
    if (!Array.isArray(args)) {
      args = [args];
    }
    const uniqueID = this.config.namespace ? `${this.config.namespace}:#${this.config.buildId}` : this.config.buildId;
    return ['build', uniqueID, ...args].join(':');
  }

  private createRedisScripts() {
    return {
      acknowledge: defineScript({
        NUMBER_OF_KEYS: 3,
        SCRIPT: readFileSync(`${__dirname}/../../../redis/acknowledge.lua`).toString(),
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
        SCRIPT: readFileSync(`${__dirname}/../../../redis/requeue.lua`).toString(),
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
        SCRIPT: readFileSync(`${__dirname}/../../../redis/release.lua`).toString(),
        transformArguments(setKey: string, workerQueueKey: string, ownersKey: string) {
          return [setKey, workerQueueKey, ownersKey];
        },
      }),

      reserve: defineScript({
        NUMBER_OF_KEYS: 5,
        SCRIPT: readFileSync(`${__dirname}/../../../redis/reserve.lua`).toString(),
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
        transformReply(reply: string | null | undefined) {
          return reply;
        },
      }),
      reserveLost: defineScript({
        NUMBER_OF_KEYS: 4,
        SCRIPT: readFileSync(`${__dirname}/../../../redis/reserve_lost.lua`).toString(),
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
        transformReply(reply: string | null | undefined) {
          return reply;
        },
      }),
    };
  }
}
