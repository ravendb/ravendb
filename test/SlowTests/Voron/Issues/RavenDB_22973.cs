using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_22973 : StorageTest
{
    public RavenDB_22973(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);

        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ScratchBufferMustNotContainDeletedLeftoversAfterFlushing_AllocateFreeAllocateInSameTx()
    {
        using (var txw = Env.WriteTransaction())
        {
            var llt = txw.LowLevelTransaction;

            var p1 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

            llt.ModifyPage(p1);

            llt.FreePage(p1);

            var p11 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

            Assert.Equal(p1, p11); // the same page number was returned by free space handling

            llt.ModifyPage(p11);

            txw.Commit();
        }

        Env.FlushLogToDataFile();

        var scratchBufferFile = Env.ScratchBufferPool.GetScratchBufferFile(0);

        Assert.Equal(0, scratchBufferFile.AllocatedPagesCount);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ScratchBufferMustNotContainDeletedLeftoversAfterFlushing_AllocateInOneTx_FreeAndAllocateInNextTx()
    {
        long p1;

        using (var txw = Env.WriteTransaction())
        {
            var llt = txw.LowLevelTransaction;

            p1 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

            llt.ModifyPage(p1);

            txw.Commit();
        }

        using (var txw = Env.WriteTransaction())
        {
            var llt = txw.LowLevelTransaction;
            llt.FreePage(p1);

            var p11 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

            Assert.Equal(p1, p11); // the same page number was returned by free space handling

            llt.ModifyPage(p11);

            txw.Commit();
        }

        Env.FlushLogToDataFile();

        var scratchBufferFile = Env.ScratchBufferPool.GetScratchBufferFile(0);

        Assert.Equal(0, scratchBufferFile.AllocatedPagesCount);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ScratchBufferMustNotContainDeletedLeftoversAfterFlushing_AllocateInOneTx_FreeAndAllocateInNextTxWrappedByReadTx()
    {
        long p1;

        using (var txw = Env.WriteTransaction())
        {
            var llt = txw.LowLevelTransaction;

            p1 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

            llt.ModifyPage(p1);

            txw.Commit();
        }

        using (var readTx = Env.ReadTransaction())
        {
            var readPage = readTx.LowLevelTransaction.GetPage(p1);

            using (var txw = Env.WriteTransaction())
            {
                var llt = txw.LowLevelTransaction;
                llt.FreePage(p1);

                var p11 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

                Assert.Equal(p1, p11); // the same page number was returned by free space handling
                    
                llt.ModifyPage(p11);

                txw.Commit();
            }

            Env.FlushLogToDataFile();

            var scratchBufferFile = Env.ScratchBufferPool.GetScratchBufferFile(0);

            Assert.Equal(readTx.LowLevelTransaction.CurrentStateRecord.ScratchPagesTable.Count, scratchBufferFile.AllocatedPagesCount);
        }
    }
}
