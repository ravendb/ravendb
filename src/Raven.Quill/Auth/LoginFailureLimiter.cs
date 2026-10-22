using System.Threading.RateLimiting;

namespace Raven.Quill.Auth;

public sealed class LoginFailureLimiter(TimeSpan window) : IAsyncDisposable
{
    internal const int MaxFailures = 10;
    internal static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    private readonly PartitionedRateLimiter<string> _failures = PartitionedRateLimiter.Create<string, string>(client =>
        RateLimitPartition.GetFixedWindowLimiter(client, _ => new FixedWindowRateLimiterOptions
        {
            PermitLimit = MaxFailures,
            Window = window,
            QueueLimit = 0,
            AutoReplenishment = true,
        }));

    public LoginFailureLimiter() : this(Window)
    {
    }

    public bool IsThrottled(string client) => _failures.GetStatistics(client)?.CurrentAvailablePermits == 0;

    public bool RegisterFailure(string client)
    {
        using var lease = _failures.AttemptAcquire(client);
        return lease.IsAcquired == false;
    }

    public ValueTask DisposeAsync() => _failures.DisposeAsync();
}
