using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresSnowflakeRetryTheoryAttribute : RetryTheoryAttribute
{
    public RequiresSnowflakeRetryTheoryAttribute(int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
        : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (SnowflakeConnectionString.Instance.CanConnect == false)
            Skip = "Test requires online Snowflake database";
    }
}
