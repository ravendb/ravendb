using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;

namespace Tests.Infrastructure;

public class RequiresRabbitMqFactAttribute : RetryFactAttribute
{
    internal static readonly bool CanConnect;

    static RequiresRabbitMqFactAttribute()
    {
        CanConnect = RabbitMqConnectionString.Instance.CanConnect();
    }

    public RequiresRabbitMqFactAttribute(int maxRetries = 3,
        int delayBetweenRetriesMs = 1000,
        params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        if (CanConnect == false)
            Skip = "Test requires RabbitMQ instance";
    }
}
