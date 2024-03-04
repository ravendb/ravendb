using System;
using xRetry;

namespace Tests.Infrastructure;

public class RavenIntegrationRetryFactAttribute : RetryFactAttribute
{
    public RavenIntegrationRetryFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI == false)
        {
            Skip = "Integration tests are supposed to run only on CI.";
            return;
        }
    }
}
