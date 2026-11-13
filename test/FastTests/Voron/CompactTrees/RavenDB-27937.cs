using System;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Xunit;

namespace FastTests.Voron.CompactTrees;

public class RavenDB_27937 : StorageTest
{
    public RavenDB_27937(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReinitializingAKeyKeepsItsBuffers()
    {
        // EntryTermsReader initializes the key it is given for every entry it reads
        using var rtx = Env.ReadTransaction();
        var key = new CompactKey();
        key.Initialize(rtx.LowLevelTransaction);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
            key.Initialize(rtx.LowLevelTransaction);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        key.Reset();

        // it used to rent a 2 KB and a 1 KB array on every call
        Assert.True(allocated < 64 * 1024, $"{allocated:N0} bytes allocated for 1,000 initializations");
    }
}
