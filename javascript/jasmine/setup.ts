import jasmineRequire from 'jasmine-core/lib/jasmine-core/jasmine'

const jasmine = jasmineRequire.core(jasmineRequire)
export const env = jasmine.getEnv()
const jasmineInterface = jasmineRequire.interface(jasmine, env)
Object.assign(global, jasmineInterface)