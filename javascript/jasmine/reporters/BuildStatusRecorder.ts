import jasmine from 'jasmine';

export class BuildStatusRecorder implements jasmine.CustomReporter {
  constructor() {}

  jasmineStarted(suiteInfo: jasmine.JasmineStartedInfo) {
    console.log('Running suite with ' + suiteInfo.totalSpecsDefined);
  }

  suiteStarted(result: jasmine.SuiteResult) {}

  async specStarted(result: jasmine.SpecResult) {}

  specDone(result: jasmine.SpecResult) {
    console.log('Spec: ' + result.description + ' was ' + result.status);

    for (const expectation of result.failedExpectations) {
      console.log('Failure: ' + expectation.message);
      console.log(expectation.stack);
    }

    console.log(result.passedExpectations.length);
  }

  suiteDone(result: jasmine.SuiteResult) {}

  jasmineDone(runDetails: jasmine.JasmineDoneInfo) {}
}
