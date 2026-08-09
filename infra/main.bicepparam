using './main.bicep'

param baseName = 'cairndb'
param imageTag = 'latest'
param blobContainerName = 'cairndb-data'

// Application handlers used by the snapshot job (module:attribute).
// Point this at your application's registry once you have one.
param snapshotHandlersRef = 'cairndb.client.demo_handlers:registry'

// Nightly snapshot at 03:00 UTC; weekly GC on Sunday 04:00 UTC.
param snapshotCron = '0 3 * * *'
param gcCron = '0 4 * * 0'
param gcKeepSnapshots = 3
