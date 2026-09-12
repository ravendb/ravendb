using System;
using System.Threading;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Xunit;

namespace FastTests.Voron.CompactTrees;

public class RavenDB_25281 : StorageTest
{
    public RavenDB_25281(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CompactKeyCanBeInitializedOnOneThreadAndResetOnAnother()
    {
        // CompactKey's backing arrays used to be pooled in [ThreadStatic] ArrayPool fields. Since
        // CompactKey instances are rented/returned via a process-wide ObjectPool (LowLevelTransaction's
        // _sharedCompactKeyPool), a key can legitimately be initialized (renting arrays) on one thread
        // and reset (returning arrays) on a different one. With thread-static pools, the releasing
        // thread's pool field could be null, throwing a NullReferenceException from Reset(). The pools
        // are now process-wide static readonly fields, so this cross-thread sequence must not throw.
        using var wtx = Env.WriteTransaction();

        var key = new CompactKey();

        var initThread = new Thread(() => key.Initialize(wtx.LowLevelTransaction));
        initThread.Start();
        initThread.Join();

        Exception caught = null;
        var resetThread = new Thread(() =>
        {
            try
            {
                key.Reset();
            }
            catch (Exception e)
            {
                caught = e;
            }
        });
        resetThread.Start();
        resetThread.Join();

        Assert.Null(caught);
    }
}
