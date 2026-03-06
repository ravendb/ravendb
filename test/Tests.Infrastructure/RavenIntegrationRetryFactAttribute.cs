using System;
using xRetry.v3;

namespace Tests.Infrastructure;

public class RavenIntegrationRetryFactAttribute : RetryFactAttribute
{
    public RavenIntegrationRetryFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000) : base(maxRetries, delayBetweenRetriesMs)
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
