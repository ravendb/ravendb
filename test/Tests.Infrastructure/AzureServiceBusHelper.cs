using System;
using System.Threading;
using Azure.Messaging.ServiceBus.Administration;

namespace Tests.Infrastructure;

public static class AzureServiceBusHelper
{
    private const string CannotConnectSkipMessage = "Test requires Azure Service Bus namespace.";

    private const string EnvironmentVariableNotFoundSkipMessage = $"'{RavenTestHelper.EnvironmentVariables.AzureConnectionStringEnvName}' environment variable not found.";

    private const string AdminEnvironmentVariableNotFoundSkipMessage = $"'{RavenTestHelper.EnvironmentVariables.AzureConnectionStringAdminEnvName}' environment variable not found.";

    private const double MillisecondsToWaitForAzureServiceBus = 15000;

    private static bool CanConnectToAzureServiceBus(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return false;

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

    public static string GetConnectionString() => Environment.GetEnvironmentVariable(RavenTestHelper.EnvironmentVariables.AzureConnectionStringEnvName);

    public static string GetAdminConnectionString() => Environment.GetEnvironmentVariable(RavenTestHelper.EnvironmentVariables.AzureConnectionStringAdminEnvName);

    public static bool ShouldSkip(out string skipMessage)
    {
        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
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
