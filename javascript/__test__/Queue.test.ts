let queue;
const TEST_LIST = ['ATest testFoo', 'ATest testBar', 'BTest testFoo', 'BTest testBar'];

const pollAll = async () => {
  let test;
  const pollOrder: string[] = [];
  do {
    test = await queue.poll();
    if (test) {
      pollOrder.push(test);
    }
  } while (test);
  return pollOrder;
};

describe('queue tests', () => {
  beforeEach(async () => {
    queue = 1;
  });

  it('updates queue progress on poll', () => {
    let count = 0;
    expect(queue.progress).toEqual(0);
    let test = await queue.poll();
    while (test) {
      count += 1;
      const progress = await queue.progress();
      expect(progress).toEqual(count);
      test = await queue.poll();
    }
  });

  it('updates queue size on poll', () => {
    expect(await queue.size()).toEqual(TEST_LIST.length);
    while (await queue.poll()) {}
    expect(await queue.size()).toEqual(0);
  });

  it('updates queue exhausted after all tests are polled', () => {
    expect(await queue.queueIsExhausted).toBe(false);
    await populate(queue);
    expect(await queue.queueIsExhausted).toBe(false);
    while (await queue.poll()) {}
    expect(await queue.queueIsExhausted).toBe(true);
  });

  it('converts the queue to an array', () => {
    let queueArray = await queue.toArray();
    expect(queueArray).toEqual(SHUFFLED_TEST_LIST);
    while (await queue.poll()) {}
    queueArray = await queue.toArray();
    expect(queueArray).toEqual([]);
  });

  it('polls tests in the expected order', () => {
    const pollOrder = await pollAll();
    expect(pollOrder).toEqual(SHUFFLED_TEST_LIST);
  });

  it('requeues tests on failure', () => {
    const test = await queue.poll();
    await queue.reportFailure();
    await queue.requeue(test);
    const [requeuedTest, ...restOfTests] = SHUFFLED_TEST_LIST;
    const pollOrder = await pollAll();
    expect(pollOrder).toEqual([...restOfTests, requeuedTest]);
    expect(await queue.total()).toEqual(await queue.progress());
  });

  it('acknowledges tests', () => {
    let test;
    do {
      test = await queue.poll();
      expect(await queue.acknowledge(test)).toBe(true);
    } while (test);
  });

  it('releases the queue', () => {
    await queue.release();
  });

  it('shutsdown properly', () => {
    const tests = [];
    let test;
    do {
      test = await queue.poll();
      if (test) {
        tests.push(test);
      }
      await queue.shutdown();
    } while (test);
    expect(tests).toEqual(['ATest testFoo']);
  });
});
