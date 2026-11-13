// Starter IaC (design reference; validate with `az bicep build` before use). Names must be globally unique.
param location string = resourceGroup().location
param prefix string = 'rcmdemo'

resource sa 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${prefix}dl${uniqueString(resourceGroup().id)}'
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: { isHnsEnabled: true, minimumTlsVersion: 'TLS1_2', allowBlobPublicAccess: false }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = { parent: sa, name: 'default' }

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for c in ['landing', 'bronze', 'silver', 'gold']: {
  parent: blob
  name: c
}]

resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: '${prefix}-kv-${uniqueString(resourceGroup().id)}'
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
    enableSoftDelete: true
  }
}

resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: '${prefix}-adf-${uniqueString(resourceGroup().id)}'
  location: location
  identity: { type: 'SystemAssigned' }
}
// Next: role assignments (ADF MI -> Storage Blob Data Contributor; ADF MI -> Key Vault Secrets User),
// Databricks workspace + Access Connector, diagnostic settings to Log Analytics.
