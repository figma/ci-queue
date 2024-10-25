export type InitConfig = {
  buildId: string;
  workerId: string;
  seed?: string;
  redisTTL?: number;
  maxRequeues?: number;
  requeueTolerance?: number;
  maxTestsAllowedToFail?: number;
  timeout?: number;
  reportTimeout?: number;
  inactiveWorkersTimeout?: number;
  namespace?: string;
  failureFile?: string;
};

export class Configuration {
  buildId: string;
  workerId: string;
  seed?: string;
  maxRequeues: number;
  requeueTolerance: number;
  maxTestsAllowedToFail: number;
  redisTTL: number;
  timeout: number;
  reportTimeout: number;
  inactiveWorkersTimeout: number;
  namespace?: string;
  failureFile?: string;
  constructor({
    buildId,
    workerId,
    seed,
    redisTTL,
    maxRequeues,
    requeueTolerance,
    maxTestsAllowedToFail,
    timeout,
    reportTimeout,
    inactiveWorkersTimeout,
    namespace,
    failureFile,
  }: {
    buildId: string;
    workerId: string;
    seed?: string;
    redisTTL?: number;
    maxRequeues?: number;
    requeueTolerance?: number;
    maxTestsAllowedToFail?: number;
    timeout?: number;
    reportTimeout?: number;
    inactiveWorkersTimeout?: number;
    namespace?: string;
    failureFile?: string;
  }) {
    this.buildId = buildId;
    this.workerId = workerId;
    this.seed = seed;
    this.redisTTL = redisTTL ?? 8 * 60 * 60;
    this.maxRequeues = maxRequeues ?? 0;
    this.requeueTolerance = requeueTolerance ?? 0;
    this.maxTestsAllowedToFail = maxTestsAllowedToFail ?? 0;
    this.timeout = timeout ?? 30;
    this.reportTimeout = reportTimeout ?? this.timeout;
    this.inactiveWorkersTimeout = inactiveWorkersTimeout ?? this.timeout;
    this.namespace = namespace;
    this.failureFile = failureFile;
  }

  static fromEnv() {
    const buildId =
      process.env['CIRCLE_BUILD_URL'] ||
      process.env['BUILDKITE_BUILD_ID'] ||
      process.env['TRAVIS_BUILD_ID'] ||
      process.env['HEROKU_TEST_RUN_ID'] ||
      process.env['SEMAPHORE_PIPELINE_ID'];
    const workerId =
      process.env['CIRCLE_NODE_INDEX'] ||
      process.env['BUILDKITE_PARALLEL_JOB'] ||
      process.env['CI_NODE_INDEX'] ||
      process.env['SEMAPHORE_JOB_ID'];
    const seed =
      process.env['CIRCLE_SHA1'] ||
      process.env['BUILDKITE_COMMIT'] ||
      process.env['TRAVIS_COMMIT'] ||
      process.env['HEROKU_TEST_RUN_COMMIT_VERSION'] ||
      process.env['SEMAPHORE_GIT_SHA'];
    const redisTTL = Number(process.env['CI_QUEUE_REDIS_TTL']) || 8 * 60 * 60;
    return Configuration.constructor({ buildId, workerId, seed, redisTTL });
  }

  globalMaxRequeues(testCount: number) {
    return Math.ceil(testCount * this.requeueTolerance);
  }
}
