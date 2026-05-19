using System;
using Tests.Infrastructure.ConnectionString;
using xRetry.v3;

namespace Tests.Infrastructure;

public class RequiresMsSqlRetryFactAttribute : RetryFactAttribute
{
    public RequiresMsSqlRetryFactAttribute(int maxRetries = 3, int delayBetweenRetriesMs = 0)
        : base(maxRetries, delayBetweenRetriesMs)
    {
        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
            return;

        if (MsSqlConnectionString.Instance.CanConnect == false)
            Skip = "Test requires MsSQL database";
    }
}
