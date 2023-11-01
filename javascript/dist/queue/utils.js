"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.shuffleArray = exports.sleep = void 0;
const seedrandom_1 = __importDefault(require("seedrandom"));
async function sleep(timeToSleepMs) {
    return new Promise((res) => {
        setTimeout(res, timeToSleepMs);
    });
}
exports.sleep = sleep;
function shuffleArray(arr, seed) {
    const rng = (0, seedrandom_1.default)(seed.toString());
    const arrCopy = [...arr];
    for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(rng() * (i + 1));
        [arrCopy[i], arrCopy[j]] = [arrCopy[j], arrCopy[i]];
    }
    return arrCopy;
}
exports.shuffleArray = shuffleArray;
