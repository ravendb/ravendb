using System;
using System.Threading;
using Azure.Messaging.ServiceBus.Administration;

namespace Tests.Infrastructure;

public static class AzureServiceBusHelper
{
    private const string ConnectionStringEnvironmentVariable = "RAVEN_AZURE_SERVICE_BUS_CONNECTION_STRING";

    private const string ConnectionStringAdminEnvironmentVariable = "RAVEN_AZURE_SERVICE_BUS_ADMIN_CONNECTION_STRING";

    private const string CannotConnectSkipMessage = "Test requires Azure Service Bus namespace.";

    private const string EnvironmentVariableNotFoundSkipMessage = $"'{ConnectionStringEnvironmentVariable}' environment variable not found.";

    private const string AdminEnvironmentVariableNotFoundSkipMessage = $"'{ConnectionStringAdminEnvironmentVariable}' environment variable not found.";

    private const double MillisecondsToWaitForAzureServiceBus = 15000;

    private static bool CanConnectToAzureServiceBus(string connectionString)
    {
        try
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(MillisecondsToWaitForAzureServiceBus)))
            {
                var adminClient = new ServiceBusAdministrationClient(connectionString);
                var task = adminClient.GetNamespacePropertiesAsync(cts.Token);
                task.Wait();
                return task.IsCompletedSuccessfully && task.Result.Value != null;
            }
        }
        catch
        {
            return false;
        }
    }

    public static string GetConnectionString() => Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);

    public static string GetAdminConnectionString() => Environment.GetEnvironmentVariable(ConnectionStringAdminEnvironmentVariable);

    public static bool ShouldSkip(out string skipMessage)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (string.IsNullOrEmpty(GetConnectionString()))
        {
            skipMessage = EnvironmentVariableNotFoundSkipMessage;
            return true;
        }

        if (string.IsNullOrEmpty(GetAdminConnectionString()))
        {
            skipMessage = AdminEnvironmentVariableNotFoundSkipMessage;
            return true;
        }

        if (CanConnectToAzureServiceBus(GetAdminConnectionString()))
        {
            skipMessage = null;
            return false;
        }

        skipMessage = CannotConnectSkipMessage;
        return true;
    }
}
