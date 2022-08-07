using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresKafkaTheoryAttribute : RetryTheoryAttribute
{
    internal static readonly bool CanConnect;

    static RequiresKafkaTheoryAttribute()
    {
        CanConnect = KafkaConnectionString.Instance.CanConnect();
    }

    public RequiresKafkaTheoryAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (CanConnect == false)
            Skip = "Test requires Kafka instance";
    }
}
