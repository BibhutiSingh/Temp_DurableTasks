targetScope = 'subscription'

@description('Deployment environment, for example dev, test, prod.')
param environmentName string = 'djp-dev'

@description('Azure region.')
param location string = deployment().location

@description('Resource group name.')
param resourceGroupName string = 'rg-${environmentName}'

@description('Flex Consumption maximum instance count.')
@minValue(40)
@maxValue(1000)
param maximumInstanceCount int = 40

@allowed([512, 2048, 4096])
param instanceMemoryMB int = 2048


var token = toLower(uniqueString(subscription().id, environmentName, location))
var storageName = 'djp${take(token, 20)}'
var planName = 'asp-djp-${take(token, 12)}'
var functionName = 'func-djp-${take(token, 20)}'
var workspaceName = 'law-djp-${take(token, 20)}'
var appInsightsName = 'appi-djp-${take(token, 20)}'
var deploymentContainerName = 'app-package-${take(token, 20)}'

resource rg 'Microsoft.Resources/resourceGroups@2024-11-01' = {
  name: resourceGroupName
  location: location
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  scope: rg
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    publicNetworkAccess: 'Enabled'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  scope: storage
}

resource deploymentContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${storage.name}/default/${deploymentContainerName}'
  dependsOn: [blobService]
  properties: {
    publicAccess: 'None'
  }
}

resource pipelineOutputContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: '${storage.name}/default/pipeline-job-output'
  dependsOn: [blobService]
  properties: {
    publicAccess: 'None'
  }
}

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' = {
  name: 'default'
  scope: storage
}

resource queueService 'Microsoft.Storage/storageAccounts/queueServices@2023-05-01' = {
  name: 'default'
  scope: storage
}

resource pipelinesTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  name: '${storage.name}/default/DjpPipelines'
  dependsOn: [tableService]
}

resource runsTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  name: '${storage.name}/default/DjpRuns'
  dependsOn: [tableService]
}

resource jobsTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  name: '${storage.name}/default/DjpJobs'
  dependsOn: [tableService]
}

resource pipelineQueue 'Microsoft.Storage/storageAccounts/queueServices/queues@2023-05-01' = {
  name: '${storage.name}/default/pipeline-runs'
  dependsOn: [queueService]
  properties: {}
}

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  scope: rg
  location: location
  properties: {
    retentionInDays: 30
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
  sku: {
    name: 'PerGB2018'
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  scope: rg
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: workspace.id
    DisableLocalAuth: true
  }
}

resource plan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: planName
  scope: rg
  location: location
  kind: 'functionapp'
  sku: {
    name: 'FC1'
    tier: 'FlexConsumption'
  }
  properties: {
    reserved: true
  }
}

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: functionName
  scope: rg
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: plan.id
    functionAppConfig: {
      deployment: {
        storage: {
          type: 'blobContainer'
          value: 'https://${storage.name}.blob.core.windows.net/${deploymentContainerName}'
          authentication: {
            type: 'SystemAssignedIdentity'
          }
        }
      }
      runtime: {
        name: 'dotnet-isolated'
        version: '8.0'
      }
      scaleAndConcurrency: {
        maximumInstanceCount: maximumInstanceCount
        instanceMemoryMB: instanceMemoryMB
      }
    }
    siteConfig: {
      http20Enabled: true
      minTlsVersion: '1.2'
    }
  }
  dependsOn: [deploymentContainer]
}

resource appSettings 'Microsoft.Web/sites/config@2023-12-01' = {
  parent: functionApp
  name: 'appsettings'
  properties: {
    FUNCTIONS_EXTENSION_VERSION: '~4'
    FUNCTIONS_WORKER_RUNTIME: 'dotnet-isolated'
    AzureWebJobsStorage__credential: 'managedidentity'
    AzureWebJobsStorage__blobServiceUri: 'https://${storage.name}.blob.core.windows.net'
    AzureWebJobsStorage__queueServiceUri: 'https://${storage.name}.queue.core.windows.net'
    AzureWebJobsStorage__tableServiceUri: 'https://${storage.name}.table.core.windows.net'
    APPLICATIONINSIGHTS_CONNECTION_STRING: appInsights.properties.ConnectionString
    APPLICATIONINSIGHTS_AUTHENTICATION_STRING: 'Authorization=AAD'
    PIPELINE_TABLE_SERVICE_URI: 'https://${storage.name}.table.core.windows.net'
    PIPELINE_BLOB_SERVICE_URI: 'https://${storage.name}.blob.core.windows.net'
    PIPELINE_MAX_CONCURRENT_JOBS: '4'
  }
  dependsOn: [functionApp]
}

var blobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var queueContributorRoleId = '974c5e8b-45b9-4653-ba55-5f855dd0fb88'
var tableContributorRoleId = '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
var metricsPublisherRoleId = '3913510d-42f4-4e42-8a64-420c390055eb'

resource blobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, functionApp.identity.principalId, blobDataContributorRoleId)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobDataContributorRoleId)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource queueRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, functionApp.identity.principalId, queueContributorRoleId)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', queueContributorRoleId)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource tableRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, functionApp.identity.principalId, tableContributorRoleId)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', tableContributorRoleId)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource metricsRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(appInsights.id, functionApp.identity.principalId, metricsPublisherRoleId)
  scope: appInsights
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', metricsPublisherRoleId)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output resourceGroupName string = rg.name
output functionAppName string = functionApp.name
output storageAccountName string = storage.name
output tableServiceUri string = 'https://${storage.name}.table.core.windows.net'
output blobServiceUri string = 'https://${storage.name}.blob.core.windows.net'
output queueName string = 'pipeline-runs'
output applicationInsightsConnectionString string = appInsights.properties.ConnectionString
