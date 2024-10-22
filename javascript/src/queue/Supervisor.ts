import { BaseRunner } from './BaseRunner';
import { Configuration } from './Configuration';
import { sleep } from './utils';

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
        console.log('Workers are active');
        timeLeftWithNoWorkers = this.config.inactiveWorkersTimeout;
      } else {
        timeLeftWithNoWorkers -= 1;
      }
    }

    if (timeLeftWithNoWorkers <= 0) {
      console.log('Aborting, it seems all workers died.');
    }
    return await this.isExhausted();
  }

  private async workersAreActive() {
    const zRangeByScoreArr = await this.client.zRangeByScore(
      this.key('running'),
      Date.now() - this.config.timeout*1000,
      '+inf',
      {
        LIMIT: { offset: 0, count: 1 },
      },
    );
    return Number(zRangeByScoreArr[0]) > 0;
  }
}
