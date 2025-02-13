import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
import { shuffleArray, sleep } from './utils';
import { TestSpec } from './Test';

export class Worker extends BaseRunner {
  private shutdownRequired: boolean = false;
  private currentlyReservedTest: string | undefined | null;
  constructor(redisUrl: string, config: Configuration) {
    super(redisUrl, config);
  }

  async *pollIter() {
    let lastLogTime = 0;
    await this.waitForMaster();
    while (
      !this.shutdownRequired &&
      !(await this.isExhausted()) &&
      !(await this.maxTestsFailed())
    ) {
      const test = await this.reserveTest();
      if (test) {
        console.log(`[ci-queue] Reserved test: ${test}`);
        yield test;
      } else {
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
        await sleep(500);
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

  async populate(tests: TestSpec[], seed?: number): Promise<boolean> {
    if (this.config.retriedBuildId) {
      console.log(`[ci-queue] Retrying failed tests for build ${this.config.retriedBuildId}`);
      const failedTestGroups = await this.getFailedTestGroupsFromPreviousBuild();
      if (failedTestGroups.length === 0) {
        console.log(`[ci-queue] No failed tests found for build ${this.config.retriedBuildId}`);
        return false;
      }
      console.log(`[ci-queue] Failed test groups: ${failedTestGroups}`);
      const failedTestGroupTimeouts: TestSpec[] = []
      for (const failedTestGroup of failedTestGroups) {
        const testSpec = tests.find((test) => test.name === failedTestGroup)
        if (testSpec) {
          failedTestGroupTimeouts.push(testSpec)
        } else {
          console.log("Failed to find timeout for test group", failedTestGroup, ". defaulting to", this.config.timeout)
          failedTestGroupTimeouts.push({ name: failedTestGroup, timeout: this.config.timeout })
        }
      }
      await this.push(failedTestGroupTimeouts);
      return true;
    }

    console.log(`[ci-queue] Populating tests`);
    if (seed !== undefined) {
      tests = shuffleArray(tests, seed);
    }
    await this.push(tests);
    return true;
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
        `[ci-queue] Currently reserved test found for worker ${this.config.workerId}:`,
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
      this.testGroupTimeoutKey(),
      Date.now() / 1000,
      this.useDynamicDeadline(),
    );
  }

  private async tryToReserveLostTest() {
    const lostTest = await this.client.reserveLost(
      this.key('running'),
      this.key('completed'),
      this.key('worker', this.config.workerId, 'queue'),
      this.key('owners'),
      this.testGroupTimeoutKey(),
      Date.now() / 1000,
      this.config.timeout,
      this.useDynamicDeadline(),
    );
    return lostTest;
  }

  private async push(testSpecs: TestSpec[]) {
    const tests = testSpecs.map((testSpec) => testSpec.name)
    const testTimeoutMap: Record<string, number> = testSpecs.reduce((acc: Record<string, number>, testSpec: TestSpec) => {
      acc[testSpec.name] = testSpec.timeout;
      return acc;
    }, {})

    this.totalTestCount = tests.length;
    this.isMaster = await this.client.setNX(this.key('master-status'), 'setup');
    if (this.isMaster) {
      await this.client
        .multi()
        .lPush(this.key('queue'), tests)
        .hSet(this.testGroupTimeoutKey(), testTimeoutMap)
        .set(this.key('total'), this.totalTestCount)
        .set(this.key('master-status'), 'ready')
        .expire(this.key('queue'), this.config.redisTTL)
        .expire(this.key('total'), this.config.redisTTL)
        .expire(this.key('master-status'), this.config.redisTTL)
        .expire(this.testGroupTimeoutKey(), this.config.redisTTL)
        .exec();
    }
    await this.client.sAdd(this.key('workers'), [this.config.workerId]);
    await this.client.expire(this.key('workers'), this.config.redisTTL);
  }

  private async shouldShutdownEarly() {
    const totalWorkers = await this.client.sMembers(this.key('workers'));
    const totalWorkersCount = totalWorkers.length;

    // Only keep 20% of workers running
    const stayRunningThreshold = Math.max(Math.floor(totalWorkersCount * 0.2), 1);
    const parallelJob = parseInt(this.config.workerId, 10);
    const shouldShutdownEarly = parallelJob > stayRunningThreshold;

    return shouldShutdownEarly;
  }
}
