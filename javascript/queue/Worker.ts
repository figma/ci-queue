import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
import { sleep } from './utils';

class Worker extends BaseRunner {
  private shutdownRequired: boolean = false;
  private currentlyReservedTest?: string;
  private totalTestCount: number = 0;
  constructor(redisUrl: string, config: Configuration) {
    super(redisUrl, config);
  }

  async *poll() {
    await this.waitForMaster();
    while (
      !this.shutdownRequired &&
      !(await this.queueIsExhausted()) &&
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

  async acknowledge(test: any): Promise<boolean> {
    const testName = test.id;
    this.throwOnMismatchingTest(testName);
    return (
      (await this.client.acknowledge(
        this.key('running'),
        this.key('processed'),
        this.key('owners'),
        testName,
      )) === 1
    );
  }

  async requeue(test: any, offset: number) {
    const testName = test.id;
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

  private throwOnMismatchingTest(testName: string) {
    if (this.currentlyReservedTest === testName) {
      this.currentlyReservedTest = undefined;
    } else {
      throw new Error(`Acknowledged ${testName} but ${this.currentlyReservedTest} was reserved`);
    }
  }

  private async reserveTest() {
    if (this.currentlyReservedTest) {
      throw new Error(
        `${this.currentlyReservedTest} is already reserver. You have to acknowledge it before you can reserver another one`,
      );
    }

    const reservedTest = (await this.tryToReserveLostTest()) ?? (await this.tryToReserveTest());
    if (!reservedTest || typeof reservedTest !== 'string') {
      throw new Error('Failed to reserve test');
    }
    this.currentlyReservedTest = reservedTest;
    return reservedTest;
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
    if (lostTest) {
      // TODO: Record warning about recovered lost test
    }
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
