# ciqueue

See the top-level README for more details around the advantages of CI queue and how it works.

## Typescript Implementation
This is a basic implementation of CI queue. It provides a Jasmine example [here](https://github.com/figma/ci-queue/blob/master/javascript/README.md#L10), but can also be integrated into other Typescript testing frameworks. To use it for a custom framework:

1. The queue needs to be initialized using a Redis URL and a configuration.
2. The queue needs to be populated with unique strings for all the tests. This can be pre-sorted so that the slowest tests are executed first.
3. Each [`Worker`](https://github.com/figma/ci-queue/blob/master/javascript/src/queue/Worker.ts#L1) should use `queue.pollIter` to get tests from the queue and execute them. Once a test is complete, it should be acknowledged using `queue.acknowledge`. If it failed, `queue.recordFailedTest` should be used. 
4. Optionally, failed tests can be requeued using `queue.requeue`. If this is the case, successful tests also need to be recorded using `queue.recordPassingTest`. 
5. To ensure that all reporting is centralized and that a global timeout is enforced, set up a [`Supervisor`](https://github.com/figma/ci-queue/blob/master/javascript/src/queue/Supervisor.ts#L1), which does not run any tests. It waits for all workers to complete and fails if the timeout is exceeded. It can provide data on the number of failed tests and write the failures to a provided JSON file. 
6. If a test takes longer than the configured timeout to be acknowledged, it is assumed to be lost, and another worker will pick it up.