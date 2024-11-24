using System;
using System.Threading;
using FastTests.Voron;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23085(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanAccountForLockedMemory()
    {
        long locked = 0;
        int currentThreadManagedThreadId = Thread.CurrentThread.ManagedThreadId;
        EventHandler<long> handler = (s, amount) =>
        {
            if (currentThreadManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                return; // we'll only update on changed in _this_ thread 
            locked += amount;
        };
        // problematic - since this is a global state
        RequireFileBasedPager();
        try
        {
            MemoryLockUsage.MemoryLockedCalled += handler;
            
            (Pager pager, Pager.State state) = Pager.Create(Options,
                Options.BasePath.Combine("data.locked").FullPath,64*1024, 
                Pal.OpenFileFlags.LockMemory | Pal.OpenFileFlags.WritableMap);
            Assert.NotEqual(0, locked);
            var prev = locked;
            pager.EnsureContinuous(ref state, 1028, 1);
            Assert.True(prev < locked);
            pager.Dispose();
            state.Dispose();
            Assert.Equal(0, locked);
        }
        finally
        {
            MemoryLockUsage.MemoryLockedCalled -= handler;
        }
       
    }
}
