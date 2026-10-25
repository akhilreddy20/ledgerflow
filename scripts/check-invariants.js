#!/usr/bin/env node
/** Print the ledger's double-entry invariant report and exit non-zero on any violation. */
const LEDGER = process.env.LEDGER_URL ?? 'http://localhost:3001';

fetch(`${LEDGER}/admin/invariants`)
  .then((r) => r.json())
  .then((report) => {
    for (const c of report.checks) console.log(`${c.ok ? '✔' : '✘'} ${c.name}${c.ok ? '' : ` -> ${JSON.stringify(c.detail)}`}`);
    console.log(`\n${report.stats.transactions} transactions, ${report.stats.entries} journal entries, ${report.stats.accounts} accounts`);
    console.log(report.ok ? 'Books balance: zero drift.' : 'INVARIANT VIOLATION');
    process.exit(report.ok ? 0 : 1);
  })
  .catch((e) => {
    console.error(`Could not reach ledger at ${LEDGER}: ${e.message}`);
    process.exit(1);
  });
