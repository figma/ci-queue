import { env } from './setup';
import './tests/index.test';
import { Worker, Configuration } from '../src/index';
import Jasmine from 'jasmine';

function getAllSpecIds(topSuite: any) {
  const allSpecNames: string[] = [];
  const queue = [topSuite];
  while (queue.length > 0) {
    const suite = queue.shift();
    for (const child of suite.children) {
      if (!child.children) {
        // It's a spec
        allSpecNames.push(child.id);
      } else {
        queue.push(child);
      }
    }
  }
  console.log('All Spec Names:', allSpecNames);
  return allSpecNames;
}

async function run() {
  const config = new Configuration({
    buildId: 'build-id',
    workerId: `worker-${Math.floor(Math.random() * 100)}`,
  });
  // TODO: Don't hardcode me
  const redisUrl = 'redis://localhost:6379';
  const queue = new Worker(redisUrl, config);
  await queue.connect();

  process.on('SIGTERM', queue.shutdown);
  process.on('SIGINT', queue.shutdown);

  // TODO: Set up reports for env

  const tests = getAllSpecIds(env.topSuite());
  await queue.populate(tests);
  for await (const test of queue.pollIter()) {
    const { overallStatus } = await env.execute([test]);
    const failed = overallStatus === 'failed';
    let requeued = false;
    if (failed && (await queue.requeue(test))) {
      requeued = true;
      console.log('Requeued the failed test');
    } else if ((await queue.acknowledge(test)) || !failed) {
      console.log('Acknowledged test');
    }

    if (!requeued && failed) {
      console.log('Test failed');
      await queue.recordFailedTest(test, "Suite");
    }
  }
  await queue.disconnect();
}
const consoleReporter = Jasmine.ConsoleReporter();
consoleReporter.setOptions({ print: console.log });
env.addReporter(consoleReporter);
env.configure({ autoCleanClosures: false, random: false });
run()
  .then(() => console.log('Done'))
  .catch((e) => console.log(e));
