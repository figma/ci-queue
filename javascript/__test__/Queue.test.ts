import { createClient } from 'redis';
import { Configuration, InitConfig } from '../queue/Configuration';
import { Worker } from '../queue/Worker';
import { shuffleArray, sleep } from '../queue/utils';

jest.setTimeout(10000);

const CONFIG = new Configuration({
  buildId: 'BUILD_ID',
  workerId: 'WORKER_ID',
  maxRequeues: 1,
  requeueTolerance: 0.1,
  maxTestsAllowedToFail: 10,
  timeout: 0.2,
});
const TEST_LIST = [
  'ATest testFoo',
  'ATest testBar',
  'BTest testFoo',
  'ATest testBaz',
  'BTest testBar',
];
const SHUFFLED_TEST_LIST = shuffleArray(TEST_LIST, 0);

let queue: Worker = new Worker('redis://localhost:6379', CONFIG);
const redisClient = createClient({ url: 'redis://localhost:6379' });

const createWorker = async (initConfig: Partial<InitConfig>, shouldPopulate: boolean = true) => {
  const config = new Configuration({
    buildId: 'BUILD_ID',
    workerId: 'WORKER_ID',
    maxRequeues: 1,
    requeueTolerance: 0.1,
    maxTestsAllowedToFail: 10,
    timeout: 0.2,
    ...initConfig,
  });
  const worker = new Worker('redis://localhost:6379', config);
  await worker.connect();
  if (shouldPopulate) {
    await populate(worker);
  }
  return worker;
};

const pollAll = async () => {
  const pollOrder: string[] = [];
  for await (const test of queue.pollIter()) {
    await queue.acknowledge(test);
    pollOrder.push(test);
  }
  return pollOrder;
};

const populate = async (queue: Worker, tests: string[] = TEST_LIST) => {
  await queue.populate(tests, 0);
};

beforeAll(async () => {
  await redisClient.connect();
});

afterAll(async () => {
  await redisClient.disconnect();
});

describe('queue tests', () => {
  beforeEach(async () => {
    queue = new Worker('redis://localhost:6379', CONFIG);
    await queue.connect();
    await populate(queue);
  });

  afterEach(async () => {
    await redisClient.flushDb();
    await queue.disconnect();
  });

  test('poll updates queue progress', async () => {
    let count = 0;
    const progress = await queue.progress();
    expect(await queue.progress()).toEqual(0);
    for await (const test of queue.pollIter()) {
      count += 1;
      await queue.acknowledge(test);
      expect(await queue.progress()).toEqual(count);
    }
  });

  test('poll updates queue size', async () => {
    expect(await queue.size()).toEqual(TEST_LIST.length);
    for await (const test of queue.pollIter()) {
      await queue.acknowledge(test);
    }
    expect(await queue.size()).toEqual(0);
  });

  test('queue is exhausted after all tests are polled', async () => {
    // We need to disconnect the original queue from the beforeEach
    await queue.disconnect();

    queue = new Worker('redis://localhost:6379', CONFIG);
    await queue.connect();
    expect(await queue.isExhausted()).toBe(false);
    await populate(queue);
    expect(await queue.isExhausted()).toBe(false);
    for await (const test of queue.pollIter()) {
      await queue.acknowledge(test);
    }
    expect(await queue.isExhausted()).toBe(true);
  });

  test('toArray converts the queue to an array', async () => {
    let queueArray = await queue.toArray();
    expect(queueArray).toEqual(SHUFFLED_TEST_LIST);
    for await (const test of queue.pollIter()) {
      await queue.acknowledge(test);
    }
    queueArray = await queue.toArray();
    expect(queueArray).toEqual([]);
  });

  test('poll returns the tests in the expected order', async () => {
    const pollOrder = await pollAll();
    expect(pollOrder).toEqual(SHUFFLED_TEST_LIST);
  });

  test('tests are requeued on failure', async () => {
    const test = await queue.poll();
    await queue.requeue(test!);
    const [requeuedTest, ...restOfTests] = SHUFFLED_TEST_LIST;
    const pollOrder = await pollAll();
    expect(pollOrder).toEqual([...restOfTests, requeuedTest]);
    expect(queue.totalTestCount).toEqual(await queue.progress());
  });

  test('acknowledging tests returns true if this worker is the first to acknowledge', async () => {
    for await (const test of queue.pollIter()) expect(await queue.acknowledge(test)).toBe(true);
  });

  test('releasing the queue does not error', async () => {
    await queue.release();
  });

  test('shutting down the queue ', async () => {
    const tests = [];
    for await (const test of queue.pollIter()) {
      tests.push(test);
      await queue.shutdown();
    }
    expect(tests).toEqual([SHUFFLED_TEST_LIST[0]]);
  });

  test('a new worker can be created from a redis uri', async () => {
    const secondWorker = new Worker('redis://localhost:6379', CONFIG);
    await secondWorker.connect();
    await populate(secondWorker);
    expect(await secondWorker.toArray()).toEqual(await queue.toArray());
    await secondWorker.disconnect();
  });

  test('requeuing a test works with an offset', async () => {
    // Offset can be a little confusing because we pop
    // from the back of the queue. Since offset is used as a
    // negative index and we put the requeued test BEFORE the pivot,
    // the requeued test will actually run after the pivot
    const firstTest = SHUFFLED_TEST_LIST[0];
    const testOrder = [];
    let alreadyFailed = false;
    for await (const test of queue.pollIter()) {
      testOrder.push(test);
      if (test === firstTest && !alreadyFailed) {
        alreadyFailed = true;
        await queue.requeue(test, 2);
      } else {
        await queue.acknowledge(test);
      }
    }
    const expectedOrder = [...SHUFFLED_TEST_LIST];
    expectedOrder.splice(-1, 0, firstTest);
    expect(testOrder).toEqual(expectedOrder);
  });

  test('master election', async () => {
    expect(queue.isMaster).toBe(true);
    let secondWorker = await createWorker({ workerId: '2' });
    expect(secondWorker.isMaster).toBe(false);

    await redisClient.flushDb();
    await secondWorker.disconnect();
    secondWorker = await createWorker({ workerId: '2' });
    expect(secondWorker.isMaster).toBe(true);
    const thirdWorker = await createWorker({ workerId: '3' });
    expect(thirdWorker.isMaster).toBe(false);

    await Promise.all([secondWorker.disconnect(), thirdWorker.disconnect()]);
  });

  test('the queue is not exhausted if it is not populated', async () => {
    const secondWorker = await createWorker({ workerId: '2' }, false);
    expect(await secondWorker.isExhausted()).toBe(false);

    await pollAll();

    expect(await secondWorker.isExhausted()).toBe(true);
    await secondWorker.disconnect();
  });

  test('a worker picks up timed out tests from other workers', async () => {
    const secondWorker = await createWorker({ workerId: '2' });

    let acquiredRes: (value: unknown) => void;
    const acquiredPromise = new Promise((res, _rej) => {
      acquiredRes = res;
    });
    let doneRes: (value: unknown) => void;
    const donePromise = new Promise((res, _rej) => {
      doneRes = res;
    });

    const runSecondWorker = async () => {
      await acquiredPromise;
      for await (const test of secondWorker.pollIter()) {
        await secondWorker.acknowledge(test);
      }
      doneRes(true);
    };

    const runFirstWorker = async () => {
      for await (const test of queue.pollIter()) {
        acquiredRes(true);
        await donePromise;
      }
    };

    runSecondWorker();
    await runFirstWorker();

    expect(await queue.isExhausted()).toBe(true);
    await secondWorker.disconnect();
  });

  test('releasing a worker allows for reserved tests to go back into the queue', async () => {
    const secondWorker = await createWorker({ workerId: '2' });
    const reservedTest = await queue.poll();
    expect(reservedTest).not.toBeNull();
    expect(reservedTest).not.toBeUndefined();

    const copyOfOriginalQueue = await createWorker({ workerId: 'WORKER_ID' });
    await copyOfOriginalQueue.release();

    expect(await secondWorker.poll()).toEqual(reservedTest);
    await Promise.all([secondWorker.disconnect(), copyOfOriginalQueue.disconnect()]);
  });

  test('worker does not requeue a test if it was picked up by another worker', async () => {
    const secondWorker = await createWorker({ workerId: '2' });

    let acquiredRes: (value: unknown) => void;
    const acquiredPromise = new Promise((res, _rej) => {
      acquiredRes = res;
    });
    let doneRes: (value: unknown) => void;
    const donePromise = new Promise((res, _rej) => {
      doneRes = res;
    });

    const runSecondWorker = async () => {
      await acquiredPromise;
      for await (const test of secondWorker.pollIter()) {
        await secondWorker.acknowledge(test);
      }
      doneRes(true);
    };

    let alreadyRan = false;
    const runFirstWorker = async () => {
      for await (const test of queue.pollIter()) {
        if (alreadyRan) {
          break;
        }
        acquiredRes(true);
        await donePromise;
        queue.requeue(test);
      }
    };

    runSecondWorker();
    await runFirstWorker();
    expect(await queue.isExhausted()).toBe(true);

    await secondWorker.disconnect();
  });

  test('acknowledge returns false if the test was picked up by another worker', async () => {
    const secondWorker = await createWorker({ workerId: '2' });

    let acquiredRes: (value: unknown) => void;
    const acquiredPromise = new Promise((res, _rej) => {
      acquiredRes = res;
    });
    let doneRes: (value: unknown) => void;
    const donePromise = new Promise((res, _rej) => {
      doneRes = res;
    });

    const runSecondWorker = async () => {
      await acquiredPromise;
      for await (const test of secondWorker.pollIter()) {
        expect(await secondWorker.acknowledge(test)).toBe(true);
      }
      doneRes(true);
    };

    let alreadyRan = false;
    const runFirstWorker = async () => {
      for await (const test of queue.pollIter()) {
        if (alreadyRan) {
          break;
        }
        acquiredRes(true);
        await donePromise;
        expect(await queue.acknowledge(test)).toBe(false);
      }
    };

    runSecondWorker();
    await runFirstWorker();
    expect(await queue.isExhausted()).toBe(true);

    await secondWorker.disconnect();
  });

  test('workers register', async () => {
    expect(await redisClient.sCard('build:BUILD_ID:workers')).toEqual(1);
    const secondWorker = await createWorker({ workerId: '2' });
    expect(await redisClient.sCard('build:BUILD_ID:workers')).toEqual(2);

    await secondWorker.disconnect();
  });

  test('continuously timing out tests are at some point picked up', async () => {
    for (let i = 0; i < 3; i++) {
      await redisClient.flushDb();
      const workers = [];
      const createTimeOutWorker = async (workerId: string) => {
        const worker = await createWorker(
          {
            workerId,
            buildId: 'BUILD_ID_2',
          },
          false,
        );
        await populate(worker, [TEST_LIST[0]]);
        for await (const test of worker.pollIter()) {
          await sleep(1000);
          await worker.acknowledge(test);
        }
        await worker.disconnect();
      };
      for (let j = 0; j < 2; j++) {
        workers.push(createTimeOutWorker(j.toString()));
      }

      await Promise.all(workers);

      const supervisingWorker = await createWorker({ workerId: '12', buildId: 'BUILD_ID_2' });
      expect(await supervisingWorker.isExhausted()).toBe(true);
      await supervisingWorker.disconnect();
    }
  });
});
