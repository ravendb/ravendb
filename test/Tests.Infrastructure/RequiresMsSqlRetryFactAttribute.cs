using System;
using FastTests;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresMsSqlRetryFactAttribute : RetryFactAttribute
{
    public RequiresMsSqlRetryFactAttribute(int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
        : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (MySqlConnectionString.Instance.CanConnect == false)
            Skip = "Test requires MySQL database";
    }
}
