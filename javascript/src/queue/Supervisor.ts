import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
import { sleep } from './utils';
import { writeFileSync, mkdirSync } from 'fs';
import path from 'path';
export class Supervisor extends BaseRunner {
  constructor(redisUrl: string, config: Configuration) {
    super(redisUrl, config);
    this.isMaster = false;
  }

  async totalTests() {
    await this.waitForMaster();
    return await this.client.get(this.key('total'));
  }

  async waitForWorkers() {
    await this.waitForMaster();
    let timeLeft = this.config.reportTimeout;
    let timeLeftWithNoWorkers = this.config.inactiveWorkersTimeout;
    while (
      !(await this.isExhausted()) &&
      timeLeft > 0 &&
      !(await this.maxTestsFailed()) &&
      timeLeftWithNoWorkers > 0
    ) {
      timeLeft -= 1;
      await sleep(1000);

      if (await this.workersAreActive()) {
        // Only log every 5s to reduce noise
        if (timeLeft % 5 === 0) {
          console.log('[ci-queue] Workers are active, waiting for them to complete.');
        }
        timeLeftWithNoWorkers = this.config.inactiveWorkersTimeout;
      } else {
        timeLeftWithNoWorkers -= 1;
      }
    }

    if (timeLeftWithNoWorkers <= 0) {
      console.log('[ci-queue] Aborting, it seems all workers died.');
    }

    const isExhausted = await this.isExhausted();
    if (!isExhausted) {
      console.log('[ci-queue] Not all tests have finished in time');
      return false;
    }

    if (this.config.failureFile) {
      const failedTests = await this.getFailedTests();
      const absolutePath = path.join(process.cwd(), this.config.failureFile);
      const directory = path.dirname(absolutePath);

      mkdirSync(directory, { recursive: true });
      writeFileSync(absolutePath, failedTests);
    }
    return true;
  }

  private async workersAreActive() {
    const zRangeByScoreArr = await this.client.zRangeByScore(
      this.key('running'),
      (Date.now() / 1000) - this.config.timeout,
      '+inf',
      {
        LIMIT: { offset: 0, count: 1 },
      },
    );
    return zRangeByScoreArr.length > 0;
  }
}
