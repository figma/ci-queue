"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Supervisor = void 0;
const BaseRunner_1 = require("./BaseRunner");
const utils_1 = require("./utils");
class Supervisor extends BaseRunner_1.BaseRunner {
    constructor(redisUrl, config) {
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
        while (!(await this.isExhausted()) &&
            timeLeft > 0 &&
            !(await this.maxTestsFailed()) &&
            timeLeftWithNoWorkers > 0) {
            timeLeft -= 1;
            await (0, utils_1.sleep)(1000);
            if (await this.workersAreActive()) {
                console.log('Workers are active');
                timeLeftWithNoWorkers = this.config.inactiveWorkersTimeout;
            }
            else {
                timeLeftWithNoWorkers -= 1;
            }
        }
        if (timeLeftWithNoWorkers <= 0) {
            console.log('Aborting, it seems all workers died.');
        }
        return await this.isExhausted();
    }
    async workersAreActive() {
        const zRangeByScoreArr = await this.client.zRangeByScore(this.key('running'), (Date.now() / 1000) - this.config.timeout, '+inf', {
            LIMIT: { offset: 0, count: 1 },
        });
        return zRangeByScoreArr.length > 0;
    }
}
exports.Supervisor = Supervisor;
