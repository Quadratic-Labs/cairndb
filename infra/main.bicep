@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Base name used as a prefix for all resources')
param baseName string = 'cairndb'

@description('Container image tag to deploy')
param imageTag string = 'latest'

@description('Blob container name for the commit log and snapshots')
param blobContainerName string = 'cairndb-data'

@description('''Logs to snapshot and garbage-collect, one entry each:
- log: '' for the root log, else a named log (letters, digits and '-' only: it names the jobs)
- handlers: HandlerRegistry reference (module:attribute)
- initSchema: schema initializer reference (module:attribute), creating the projection tables''')
param snapshotLogs array = [
  {
    log: ''
    handlers: 'cairndb.client.demo_handlers:registry'
    initSchema: 'cairndb.client.demo_handlers:init_schema'
  }
]

@description('Projection schema version (snapshots/v{version}/); must equal the version your projections declare')
param schemaVersion string = '1'

@description('Cron schedule for the snapshot job (UTC)')
param snapshotCron string = '0 3 * * *'

@description('Cron schedule for the GC job (UTC)')
param gcCron string = '0 4 * * 0'

@description('Number of snapshots to keep during GC')
param gcKeepSnapshots int = 3

// There are no long-running services in CairnDB v2: writers and readers
// are libraries embedded in application processes, talking only to blob
// storage. The only compute deployed here is scheduled jobs: one snapshot
// job and one GC job per entry of snapshotLogs.

// ---------- Managed Identity ----------

module identity 'modules/managed-identity.bicep' = {
  name: 'managed-identity'
  params: {
    location: location
    identityName: '${baseName}-identity'
  }
}

// ---------- Storage Account ----------

module storage 'modules/storage-account.bicep' = {
  name: 'storage-account'
  params: {
    location: location
    storageAccountName: replace('${baseName}store', '-', '')
    blobContainerName: blobContainerName
    identityPrincipalId: identity.outputs.identityPrincipalId
  }
}

// ---------- Key Vault ----------

module keyVault 'modules/key-vault.bicep' = {
  name: 'key-vault'
  params: {
    location: location
    keyVaultName: '${baseName}-kv'
    identityPrincipalId: identity.outputs.identityPrincipalId
    storageConnectionString: storage.outputs.connectionString
  }
}

// ---------- Container Registry ----------

module acr 'modules/container-registry.bicep' = {
  name: 'container-registry'
  params: {
    location: location
    registryName: replace('${baseName}acr', '-', '')
    identityPrincipalId: identity.outputs.identityPrincipalId
  }
}

// ---------- Container Apps Environment (for the jobs) ----------

module appEnv 'modules/container-apps-env.bicep' = {
  name: 'container-apps-env'
  params: {
    location: location
    baseName: baseName
  }
}

// ---------- Snapshot + GC jobs (scheduled), one pair per log ----------

module snapshotJobs 'modules/container-app-job.bicep' = [for entry in snapshotLogs: {
  name: empty(entry.log) ? 'snapshot-job' : 'snapshot-job-${entry.log}'
  params: {
    location: location
    baseName: baseName
    jobName: empty(entry.log) ? 'snapshot' : 'snapshot-${entry.log}'
    cronExpression: snapshotCron
    args: concat([
      'snapshot'
      '--handlers'
      entry.handlers
      '--init-schema'
      entry.initSchema
      '--schema-version'
      schemaVersion
    ], empty(entry.log) ? [] : [
      '--log'
      entry.log
    ])
    environmentId: appEnv.outputs.environmentId
    acrLoginServer: acr.outputs.acrLoginServer
    imageTag: imageTag
    identityId: identity.outputs.identityId
    identityClientId: identity.outputs.identityClientId
    storageConnectionStringSecretUri: keyVault.outputs.secretUri
    blobContainerName: blobContainerName
  }
}]

module gcJobs 'modules/container-app-job.bicep' = [for entry in snapshotLogs: {
  name: empty(entry.log) ? 'gc-job' : 'gc-job-${entry.log}'
  params: {
    location: location
    baseName: baseName
    jobName: empty(entry.log) ? 'gc' : 'gc-${entry.log}'
    cronExpression: gcCron
    args: concat([
      'gc'
      '--keep-snapshots'
      string(gcKeepSnapshots)
      '--schema-version'
      schemaVersion
    ], empty(entry.log) ? [] : [
      '--log'
      entry.log
    ])
    environmentId: appEnv.outputs.environmentId
    acrLoginServer: acr.outputs.acrLoginServer
    imageTag: imageTag
    identityId: identity.outputs.identityId
    identityClientId: identity.outputs.identityClientId
    storageConnectionStringSecretUri: keyVault.outputs.secretUri
    blobContainerName: blobContainerName
  }
}]

// ---------- Outputs ----------

@description('ACR login server for docker push')
output acrLoginServer string = acr.outputs.acrLoginServer

@description('Key Vault name')
output keyVaultName string = keyVault.outputs.keyVaultName

@description('Storage account name')
output storageAccountName string = storage.outputs.storageAccountName
