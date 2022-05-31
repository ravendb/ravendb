using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresKafkaFactAttribute : RetryFactAttribute
{
    private static readonly bool _canConnect;

    static RequiresKafkaFactAttribute()
    {
        _canConnect = KafkaConnectionString.Instance.CanConnect();
    }

    public RequiresKafkaFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (_canConnect == false)
            Skip = "Test requires Kafka instance";
    }
}
