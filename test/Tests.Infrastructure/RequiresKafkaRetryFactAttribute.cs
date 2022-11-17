using System;
using FastTests;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresKafkaRetryFactAttribute : RetryFactAttribute
{
    internal static readonly bool CanConnect;

    static RequiresKafkaRetryFactAttribute()
    {
        CanConnect = KafkaConnectionString.Instance.CanConnect;
    }

    public RequiresKafkaRetryFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (CanConnect == false)
            Skip = "Test requires Kafka instance";
    }
}
