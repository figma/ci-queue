"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Supervisor = void 0;
const BaseRunner_1 = require("./BaseRunner");
const utils_1 = require("./utils");
const fs_1 = require("fs");
const path_1 = __importDefault(require("path"));
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
        if (this.config.failureFile) {
            const failedTests = await this.getFailedTests();
            const absolutePath = path_1.default.join(process.cwd(), this.config.failureFile);
            const directory = path_1.default.dirname(absolutePath);
            (0, fs_1.mkdirSync)(directory, { recursive: true });
            (0, fs_1.writeFileSync)(absolutePath, failedTests);
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
