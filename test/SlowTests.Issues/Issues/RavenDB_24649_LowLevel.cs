using System;
using FastTests;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24649_LowLevel : NoDisposalNeeded
{
    public RavenDB_24649_LowLevel(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void LastQueriedTimeTrackerBasicFunctionalitiesTest()
    {
        var now = DateTime.UtcNow;
        var timeTracker = new LastQueriedTimeTracker(now, 0);

        timeTracker.UpdateElapsedSinceQueried(now + TimeSpan.FromSeconds(5));
        Assert.Equal(TimeSpan.FromSeconds(5), timeTracker.ElapsedSinceQueried);

        timeTracker.UpdateElapsedSinceQueried(now + TimeSpan.FromSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(10), timeTracker.ElapsedSinceQueried);
        
        // mark queried in the past, since this may happen only in race condition, it should reset the elapsed anyway
        timeTracker.MarkQueried(now + TimeSpan.FromSeconds(1));
        Assert.Equal(TimeSpan.Zero, timeTracker.ElapsedSinceQueried);
        
        timeTracker.UpdateElapsedSinceQueried(now + TimeSpan.FromSeconds(15));
        Assert.Equal(TimeSpan.FromSeconds(14), timeTracker.ElapsedSinceQueried);
        
        timeTracker.MarkQueried(now + TimeSpan.FromSeconds(16));
        Assert.Equal(TimeSpan.Zero, timeTracker.ElapsedSinceQueried);
        
        timeTracker.UpdateElapsedSinceQueried(now + TimeSpan.FromSeconds(16));
        Assert.Equal(TimeSpan.Zero, timeTracker.ElapsedSinceQueried);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void LastQueriedTimeTrackerRaceTest()
    {
        var now = DateTime.UtcNow;
        var timeTracker = new LastQueriedTimeTracker(now, 0);
       
        // Update to future time
        timeTracker.UpdateElapsedSinceQueried(now.AddSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(10), timeTracker.ElapsedSinceQueried);
       
        // We simulate lost race between indexing thread and query thread.
        // Query thread should always win the race since it is resetting the time.
        timeTracker.MarkQueried(now.AddSeconds(-5));
        Assert.Equal(TimeSpan.Zero, timeTracker.ElapsedSinceQueried);
        Assert.Equal(now.AddSeconds(-5), timeTracker.LastQueryDate);
        
        // Actually test that update elapsed since queried will be no-op when the race is lost
        timeTracker.UpdateElapsedSinceQueried(now.AddSeconds(-6));
        Assert.Equal(TimeSpan.Zero, timeTracker.ElapsedSinceQueried);
        Assert.Equal(now.AddSeconds(-5), timeTracker.LastQueryDate);
    }
}
