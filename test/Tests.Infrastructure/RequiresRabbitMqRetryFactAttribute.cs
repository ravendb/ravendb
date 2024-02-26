using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresRabbitMqRetryFactAttribute : RetryFactAttribute
{
    internal static readonly bool CanConnect;

    static RequiresRabbitMqRetryFactAttribute()
    {
        CanConnect = RabbitMqConnectionString.Instance.CanConnect;
    }

    public RequiresRabbitMqRetryFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (CanConnect == false)
            Skip = "Test requires RabbitMQ instance";
    }
}
