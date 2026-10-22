using FastTests;
using Raven.Quill.Auth;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class LoginFailureLimiterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly TimeSpan ShortWindow = TimeSpan.FromMilliseconds(400);

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Throttles_after_the_failure_limit_and_recovers_after_the_window()
    {
        await using var limiter = new LoginFailureLimiter(ShortWindow);

        for (var i = 0; i < LoginFailureLimiter.MaxFailures; i++)
        {
            Assert.False(limiter.IsThrottled("client"));
            Assert.False(limiter.RegisterFailure("client"));
        }

        Assert.True(limiter.IsThrottled("client"));
        Assert.True(limiter.RegisterFailure("client"));

        await Task.Delay(ShortWindow * 2);

        Assert.False(limiter.IsThrottled("client"));
        Assert.False(limiter.RegisterFailure("client"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Clients_are_counted_independently()
    {
        await using var limiter = new LoginFailureLimiter(ShortWindow);

        for (var i = 0; i <= LoginFailureLimiter.MaxFailures; i++)
            limiter.RegisterFailure("attacker");

        Assert.True(limiter.IsThrottled("attacker"));
        Assert.False(limiter.IsThrottled("operator"));
        Assert.False(limiter.RegisterFailure("operator"));
    }
}
