using './main.bicep'

param baseName = 'cairndb'
param imageTag = 'latest'
param blobContainerName = 'cairndb-data'

// One entry per log with projections (log '' = the root log). Each gets a
// snapshot job and a GC job. Point handlers/initSchema at your application's
// module:attribute references once you have them, e.g.:
//   { log: 'orders', handlers: 'myapp.projections:orders_registry', initSchema: 'myapp.projections:init_orders' }
param snapshotLogs = [
  {
    log: ''
    handlers: 'cairndb.client.demo_handlers:registry'
    initSchema: 'cairndb.client.demo_handlers:init_schema'
  }
]

// Must equal the `version` your projections declare (default "1").
param schemaVersion = '1'

// Nightly snapshot at 03:00 UTC; weekly GC on Sunday 04:00 UTC.
param snapshotCron = '0 3 * * *'
param gcCron = '0 4 * * 0'
param gcKeepSnapshots = 3
