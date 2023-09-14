import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
import { shuffleArray, sleep } from './utils';

export class Worker extends BaseRunner {
  private shutdownRequired: boolean = false;
  private currentlyReservedTest: string | undefined | null;
  constructor(redisUrl: string, config: Configuration) {
    super(redisUrl, config);
  }

  async *pollIter() {
    await this.waitForMaster();
    while (
      !this.shutdownRequired &&
      !(await this.isExhausted()) &&
      !(await this.maxTestsFailed())
    ) {
      const test = await this.reserveTest();
      if (test) {
        yield test;
      } else {
        await sleep(50);
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

  async acknowledge(test: string): Promise<boolean> {
    this.throwOnMismatchingTest(test);
    return (
      (await this.client.acknowledge(
        this.key('running'),
        this.key('processed'),
        this.key('owners'),
        test,
      )) === 1
    );
  }

  async requeue(test: string, offset: number = 42) {
    const testName = test;
    this.throwOnMismatchingTest(testName);
    const globalMaxRequeues = this.config.globalMaxRequeues(this.totalTestCount);
    const requeued =
      this.config.maxRequeues > 0 &&
      globalMaxRequeues > 0 &&
      (await this.client.requeue(
        this.key('processed'),
        this.key('requeues-count'),
        this.key('queue'),
        this.key('running'),
        this.key('worker', this.config.workerId, 'queue'),
        this.key('owners'),
        this.config.maxRequeues,
        globalMaxRequeues,
        testName,
        offset,
      )) === 1;

    if (!requeued) {
      this.currentlyReservedTest = testName;
    }
    return requeued;
  }

  async release() {
    await this.client.release(
      this.key('running'),
      this.key('worker', this.config.workerId, 'queue'),
      this.key('owners'),
    );
  }

  async populate(tests: string[], seed?: number) {
    if (seed !== undefined) {
      tests = shuffleArray(tests, seed);
    }
    await this.push(tests);
  }

  shutdown() {
    this.shutdownRequired = true;
  }

  private throwOnMismatchingTest(testName: string) {
    if (this.currentlyReservedTest === testName) {
      this.currentlyReservedTest = undefined;
    } else {
      throw new Error(`Acknowledged ${testName} but ${this.currentlyReservedTest} was reserved`);
    }
  }

  private async reserveTest(): Promise<string | undefined | null> {
    if (this.currentlyReservedTest) {
      console.error(
        `Currently reserved test found for worker ${this.config.workerId}:`,
        this.currentlyReservedTest,
      );
      throw new Error(
        `${this.currentlyReservedTest} is already reserved. You have to acknowledge it before you can reserve another one`,
      );
    }

    const reservedTest = (await this.tryToReserveLostTest()) ?? (await this.tryToReserveTest());
    this.currentlyReservedTest = reservedTest;
    return reservedTest as any;
  }

  private async tryToReserveTest() {
    return await this.client.reserve(
      this.key('queue'),
      this.key('running'),
      this.key('processed'),
      this.key('worker', this.config.workerId, 'queue'),
      this.key('owners'),
      Date.now(),
    );
  }

  private async tryToReserveLostTest() {
    const lostTest = await this.client.reserveLost(
      this.key('running'),
      this.key('completed'),
      this.key('worker', this.config.workerId, 'queue'),
      this.key('owners'),
      Date.now(),
      this.config.timeout,
    );
    return lostTest;
  }

  private async push(tests: any[]) {
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
}
