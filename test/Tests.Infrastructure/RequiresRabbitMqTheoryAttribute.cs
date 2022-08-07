using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresRabbitMqTheoryAttribute : RetryTheoryAttribute
{
    internal static readonly bool CanConnect;

    static RequiresRabbitMqTheoryAttribute()
    {
        CanConnect = RabbitMqConnectionString.Instance.CanConnect();
    }

    public RequiresRabbitMqTheoryAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (CanConnect == false)
            Skip = "Test requires RabbitMQ instance";
    }
}
