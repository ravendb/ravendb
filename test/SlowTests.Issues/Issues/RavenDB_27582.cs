using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27582 : NoDisposalNeeded
{
    public RavenDB_27582(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void InfiniteWaitMustNotBeRootedWhenTokenIsDisposedWithoutCancel()
    {
        var wait = StartAndAbandonInfiniteWait();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(wait.IsAlive);
    }

    [RavenFact(RavenTestCategory.Core)]
    public async Task InfiniteWaitMustObserveCancellation()
    {
        using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100)))
        {
            var wait = TimeoutManager.WaitFor(Timeout.InfiniteTimeSpan, cts.Token);

            Assert.Same(wait, await Task.WhenAny(wait, Task.Delay(TimeSpan.FromSeconds(10))));
        }
    }

    // Same shape as WaitForCommitIndexChange(..., Timeout.InfiniteTimeSpan, token) followed by OperationCancelToken.Dispose()
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference StartAndAbandonInfiniteWait()
    {
        var cts = new CancellationTokenSource();
        var wait = TimeoutManager.WaitFor(Timeout.InfiniteTimeSpan, cts.Token);
        cts.Dispose();
        return new WeakReference(wait);
    }
}
