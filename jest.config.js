/** Two Jest projects:
 *  - unit:        fast, no infrastructure needed
 *  - integration: needs PostgreSQL (TEST_DATABASE_URL), skipped automatically when it is not set
 */
const tsJest = ['ts-jest', { tsconfig: '<rootDir>/tsconfig.test.json' }];

module.exports = {
  testTimeout: 60000,
  projects: [
    {
      displayName: 'unit',
      testEnvironment: 'node',
      transform: { '^.+\\.ts$': tsJest },
      testMatch: ['<rootDir>/libs/**/test/**/*.spec.ts', '<rootDir>/services/**/test/**/*.spec.ts'],
      moduleNameMapper: { '^@ledgerflow/common$': '<rootDir>/libs/common/src' },
    },
    {
      displayName: 'integration',
      testEnvironment: 'node',
      transform: { '^.+\\.ts$': tsJest },
      testMatch: ['<rootDir>/services/**/test/**/*.int-spec.ts'],
      moduleNameMapper: { '^@ledgerflow/common$': '<rootDir>/libs/common/src' },
    },
  ],
};
