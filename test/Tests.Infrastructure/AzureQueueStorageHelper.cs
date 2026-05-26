using System;
using Azure.Storage.Queues;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Providers.Queue;

namespace Tests.Infrastructure;

public static class AzureQueueStorageHelper
{
    private const string CannotConnectSkipMessage = "Test requires Azurite instance with Azure Queue Storage.";

    private const string EnvironmentVariableNotFoundSkipMessage = $"'{RavenTestHelper.EnvironmentVariables.AzureQueueStorageConnectionStringEnvName}' environment variable not found.";

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

        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.AzureQueueStorageConnectionString == null)
        {
            skipMessage = EnvironmentVariableNotFoundSkipMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (CanConnectToAzurite(RavenTestHelper.EnvironmentVariables.AzureQueueStorageConnectionString))
        {
            skipMessage = null;
            return false;
        }

        skipMessage = CannotConnectSkipMessage;
        return true;
    }
}
