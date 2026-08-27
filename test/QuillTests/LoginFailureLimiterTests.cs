using Raven.Quill.Auth;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class LoginFailureLimiterTests
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Throttles_after_the_failure_limit_and_recovers_after_the_window()
    {
        var clock = new TestClock();
        var limiter = new LoginFailureLimiter(clock);

        for (var i = 0; i < LoginFailureLimiter.MaxFailures; i++)
        {
            Assert.False(limiter.IsThrottled("client"));
            Assert.False(limiter.RegisterFailure("client"));
        }

        Assert.True(limiter.RegisterFailure("client"));
        Assert.True(limiter.IsThrottled("client"));

        clock.Advance(LoginFailureLimiter.Window + TimeSpan.FromSeconds(1));

        Assert.False(limiter.IsThrottled("client"));
        Assert.False(limiter.RegisterFailure("client"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Reset_clears_the_failure_count()
    {
        var limiter = new LoginFailureLimiter(new TestClock());

        for (var i = 0; i <= LoginFailureLimiter.MaxFailures; i++)
            limiter.RegisterFailure("client");
        Assert.True(limiter.IsThrottled("client"));

        limiter.Reset("client");

        Assert.False(limiter.IsThrottled("client"));
        Assert.False(limiter.RegisterFailure("client"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void At_capacity_with_live_entries_new_clients_are_not_tracked_and_known_clients_still_throttle()
    {
        var clock = new TestClock();
        var limiter = new LoginFailureLimiter(clock);

        for (var i = 0; i <= LoginFailureLimiter.MaxFailures; i++)
            limiter.RegisterFailure("known");
        Assert.True(limiter.IsThrottled("known"));

        for (var i = 0; i < 10_001; i++)
            limiter.RegisterFailure($"attacker-{i}");

        for (var i = 0; i < 50; i++)
            Assert.False(limiter.RegisterFailure($"overflow-{i}"));
        Assert.False(limiter.IsThrottled("overflow-0"));

        Assert.True(limiter.IsThrottled("known"));
        Assert.True(limiter.RegisterFailure("known"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Expired_entries_are_purged_once_over_capacity()
    {
        var clock = new TestClock();
        var limiter = new LoginFailureLimiter(clock);

        for (var i = 0; i < 10_001; i++)
            limiter.RegisterFailure($"old-{i}");

        clock.Advance(LoginFailureLimiter.Window + TimeSpan.FromSeconds(1));

        Assert.False(limiter.RegisterFailure("fresh"));
        for (var i = 0; i < LoginFailureLimiter.MaxFailures; i++)
            limiter.RegisterFailure("fresh");
        Assert.True(limiter.IsThrottled("fresh"));
    }

    private sealed class TestClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
