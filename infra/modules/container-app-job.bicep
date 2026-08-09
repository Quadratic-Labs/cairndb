@description('Azure region for resources')
param location string

@description('Base name for resources')
param baseName string

@description('Job name suffix (e.g. snapshot, gc)')
param jobName string

@description('Cron expression for the schedule (UTC)')
param cronExpression string

@description('Arguments passed to the cairndb CLI entrypoint')
param args array

@description('Resource ID of the Container Apps Environment')
param environmentId string

@description('ACR login server (e.g. myacr.azurecr.io)')
param acrLoginServer string

@description('Container image tag')
param imageTag string

@description('Resource ID of the user-assigned managed identity')
param identityId string

@description('Client ID of the user-assigned managed identity')
param identityClientId string

@description('URI of the storage connection string secret in Key Vault')
param storageConnectionStringSecretUri string

@description('Blob container name for CairnDB storage')
param blobContainerName string

resource job 'Microsoft.App/jobs@2024-03-01' = {
  name: '${baseName}-${jobName}'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identityId}': {}
    }
  }
  properties: {
    environmentId: environmentId
    configuration: {
      triggerType: 'Schedule'
      scheduleTriggerConfig: {
        cronExpression: cronExpression
        parallelism: 1
        replicaCompletionCount: 1
      }
      replicaTimeout: 1800
      replicaRetryLimit: 1
      registries: [
        {
          server: acrLoginServer
          identity: identityId
        }
      ]
      secrets: [
        {
          name: 'storage-connection-string'
          keyVaultUrl: storageConnectionStringSecretUri
          identity: identityId
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'cairndb-${jobName}'
          image: '${acrLoginServer}/cairndb:${imageTag}'
          args: args
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            { name: 'CAIRNDB_STORAGE_TYPE', value: 'azure' }
            { name: 'CAIRNDB_AZURE_CONTAINER', value: blobContainerName }
            { name: 'CAIRNDB_AZURE_CONNECTION_STRING', secretRef: 'storage-connection-string' }
            { name: 'AZURE_CLIENT_ID', value: identityClientId }
          ]
        }
      ]
    }
  }
}

@description('Resource ID of the job')
output jobId string = job.id
