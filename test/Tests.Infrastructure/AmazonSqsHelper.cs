using Amazon.SQS;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.Queue;

namespace Tests.Infrastructure;

public static class AmazonSqsHelper
{
    private const string CannotConnectSkipMessage = "Test requires Elasticmq instance.";

    private const string EnvironmentVariableNotFoundSkipMessage = $"'{RavenTestHelper.EnvironmentVariables.AmazonSqsEmulatorUrlEnvName}' environment variable not found.";

    private static bool CanConnectToElasticmq()
    {
        AmazonSqsConnectionSettings connectionSettings = new() { UseEmulator = true};

        IAmazonSQS client =
            QueueBrokerConnectionHelper.CreateAmazonSqsClient(connectionSettings);

        try
        {
            // attempt to get the queue URL that doesn't exists
            AsyncHelpers.RunSync(() => client.GetQueueUrlAsync("connection-test"));
        }
        catch (AmazonSQSException ex) when (ex.ErrorCode == "QueueDoesNotExist")
        {
            return true;
        }

        return false;
    }

    public static bool ShouldSkip(out string skipMessage)
    {

        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.AmazonSqsEmulatorUrl == null)
        {
            skipMessage = EnvironmentVariableNotFoundSkipMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (CanConnectToElasticmq())
        {
            skipMessage = null;
            return false;
        }

        skipMessage = CannotConnectSkipMessage;
        return true;
    }
}
