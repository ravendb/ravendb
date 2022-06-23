using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresKafkaFactAttribute : RetryFactAttribute
{
    internal static readonly bool CanConnect;

    static RequiresKafkaFactAttribute()
    {
        CanConnect = KafkaConnectionString.Instance.CanConnect();
    }

    public RequiresKafkaFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (CanConnect == false)
            Skip = "Test requires Kafka instance";
    }
}
