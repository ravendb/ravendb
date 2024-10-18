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

    [Fact]
    public void FailureOnUpdatingJournalStateMustIgnoreFurtherFlushes()
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

        var throwOnUpdatingJournalState = true;

        Env.Journal.Applicator.ForTestingPurposesOnly().OnUpdateJournalStateUnderWriteTransactionLock += () =>
        {
            if (throwOnUpdatingJournalState == false)
                return;

            throwOnUpdatingJournalState = false; // do it just once

            throw new InvalidOperationException("For testing purposes");
        };

        var ex = Assert.Throws<InvalidOperationException>(() => Env.FlushLogToDataFile());

        Assert.Equal("For testing purposes", ex.Message);

        var flushWasCalled = false;

        Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFileUnderFlushingLock += () =>
        {
            flushWasCalled = true;
        };

        Env.FlushLogToDataFile();

        // originally I thought that the below assertion must be valid after second FlushLogToDataFile() call
        // although the first exception thrown there makes that the whole environment isn't in valid state so we'll prevent from
        // making more attempts to flush logs what is verified next in the test
        //
        //var scratchBufferFile = Env.ScratchBufferPool.GetScratchBufferFile(0);
        //Assert.Equal(0, scratchBufferFile.AllocatedPagesCount);

        Assert.False(flushWasCalled);
    }


    [Fact]
    public void FlushingMustIncrementTotalWrittenButUnsyncedBytes()
    {
        long p1;

        using (var txw = Env.WriteTransaction())
        {
            var llt = txw.LowLevelTransaction;

            p1 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;

            llt.ModifyPage(p1);

            txw.Commit();
        }


        Env.FlushLogToDataFile();

        Assert.True(Env.Journal.Applicator.TotalWrittenButUnsyncedBytes > 0);
    }
}
