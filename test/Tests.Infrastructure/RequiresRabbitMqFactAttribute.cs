using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresRabbitMqFactAttribute : RetryFactAttribute
{
    private static readonly bool _canConnect;

    static RequiresRabbitMqFactAttribute()
    {
        _canConnect = RabbitMqConnectionString.Instance.CanConnect();
    }

    public RequiresRabbitMqFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (_canConnect == false)
            Skip = "Test requires RabbitMQ instance";
    }
}
