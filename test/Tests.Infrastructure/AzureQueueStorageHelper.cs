using System;
using Azure.Storage.Queues;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Providers.Queue;

namespace Tests.Infrastructure;

public static class AzureQueueStorageHelper
{

    private const string ConnectionStringEnvironmentVariable = "RAVEN_AZURE_QUEUE_STORAGE_CONNECTION_STRING";
    
    private const string CannotConnectSkipMessage = "Test requires Azurite instance with Azure Queue Storage.";

    private const string EnvironmentVariableNotFoundSkipMessage = $"'{ConnectionStringEnvironmentVariable}' environment variable not found.";

    private const double MillisecondsToWaitForAzureQueueStorage = 1000;

    private static bool CanConnectToAzurite(string connectionString)
    {
        AzureQueueStorageConnectionSettings connectionSettings = new() { ConnectionString = connectionString };

        QueueServiceClient client =
            QueueBrokerConnectionHelper.CreateAzureQueueStorageServiceClient(connectionSettings);

        var propertiesAsync = client.GetPropertiesAsync();

        var success = propertiesAsync.Wait(TimeSpan.FromMilliseconds(MillisecondsToWaitForAzureQueueStorage));

        return success == true && propertiesAsync.Result.Value != null;
    }

    public static bool ShouldSkip(out string skipMessage)
    {
        
        if (RavenTestHelper.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        
        if (connectionString == null)
        {
            skipMessage = EnvironmentVariableNotFoundSkipMessage;
            return true;
        }
        
        if (RavenTestHelper.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (CanConnectToAzurite(connectionString))
        {
            skipMessage = null;
            return false;
        }

        skipMessage = CannotConnectSkipMessage;
        return true;
    }
}
