using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresSnowflakeRetryFactAttribute: RetryFactAttribute
{
    public RequiresSnowflakeRetryFactAttribute(int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
        : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (SnowflakeConnectionString.Instance.CanConnect == false)
            Skip = "Test requires Snowflake database";
    }
}
