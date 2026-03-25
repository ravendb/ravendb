using System;
using Tests.Infrastructure.ConnectionString;
using xRetry.v3;

namespace Tests.Infrastructure;

public class RequiresKafkaRetryFactAttribute : RetryFactAttribute
{
    internal static readonly bool CanConnect;

    static RequiresKafkaRetryFactAttribute()
    {
        CanConnect = KafkaConnectionString.Instance.CanConnect;
    }

    public RequiresKafkaRetryFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000) : base(maxRetries, delayBetweenRetriesMs)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (CanConnect == false)
            Skip = "Test requires Kafka instance";
    }
}
