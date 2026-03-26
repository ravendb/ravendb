using System;
using Tests.Infrastructure.ConnectionString;
using xRetry.v3;

namespace Tests.Infrastructure;

public class RequiresRabbitMqRetryFactAttribute : RetryFactAttribute
{
    public RequiresRabbitMqRetryFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000) : base(maxRetries, delayBetweenRetriesMs)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (RabbitMqConnectionString.Instance.CanConnect == false)
            Skip = "Test requires RabbitMQ instance";
    }
}
