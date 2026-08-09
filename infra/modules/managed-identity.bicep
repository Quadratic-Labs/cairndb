@description('Azure region for resources')
param location string

@description('Name of the user-assigned managed identity')
param identityName string

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

@description('Resource ID of the managed identity')
output identityId string = identity.id

@description('Client ID of the managed identity')
output identityClientId string = identity.properties.clientId

@description('Principal ID of the managed identity')
output identityPrincipalId string = identity.properties.principalId
