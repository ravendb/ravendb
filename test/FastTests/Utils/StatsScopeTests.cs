using System;
using System.Threading;
using Raven.Server.Utils.Stats;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Utils;

public class StatsScopeTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const RavenTestCategory Category = RavenTestCategory.Ai | RavenTestCategory.Etl;

    [RavenFact(Category)]
    public void Duration_Preserved_WhenSetExplicitly()
    {
        TimeSpan duration = TimeSpan.FromDays(1);

        Stats stats = new();

        var scope = new TestStatsScope(stats);
        scope.Duration = duration;
        scope.Dispose();

        Assert.Equal(duration, scope.Duration);
    }

    [RavenFact(Category)]
    public void Duration_Measured_WhenScoped()
    {
        Stats stats = new();

        var scope = new TestStatsScope(stats);
        new SpinWait().SpinOnce();
        scope.Dispose();

        Assert.True(scope.Duration > TimeSpan.Zero, "Duration should be greater than zero");
    }

    [RavenFact(Category)]
    public void Scope_Should_BeRestartable()
    {
        Stats stats = new();

        using var parent = new TestStatsScope(stats);

        const string name = "test";

        TimeSpan sleep1 = TimeSpan.FromMilliseconds(1);
        TimeSpan sleep2 = TimeSpan.FromMilliseconds(100);

        using (parent.For(name))
        {
            Thread.Sleep(sleep1);
        }

        using (parent.For(name))
        {
            Thread.Sleep(100);
        }

        Assert.True(parent.For(name).Duration > sleep1 + sleep2, "Should measure both sleeps");
    }

    private sealed class TestStatsScope(Stats stats, bool start = true) :
        StatsScope<Stats, TestStatsScope>(stats, start)
    {
        protected override TestStatsScope OpenNewScope(Stats stats, bool start) => new(stats, start);
    }

    private sealed class Stats;
}
