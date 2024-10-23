using System;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
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

    [RavenFact(RavenTestCategory.Voron)]
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


    [RavenFact(RavenTestCategory.Voron)]
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

    [RavenFact(RavenTestCategory.Voron)]
    public void Overflow_should_reuse_shrinked_page_that_got_freed()
    {
        var shrinkedPageNumber = -1L;
        PageFromScratchBuffer shrinkedPageInScratch = null;

        using (var tx = Env.WriteTransaction())
        {
            var overflowSize = 4 * Constants.Storage.PageSize;
            var page = tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);

            var allocatedPagesCount = Env.ScratchBufferPool._current.File.AllocatedPagesCount;

            var reducedOverflowSize = 2 * Constants.Storage.PageSize;

            tx.LowLevelTransaction.ShrinkOverflowPage(page.PageNumber, reducedOverflowSize, tx.LowLevelTransaction.RootObjects);

            shrinkedPageNumber = page.PageNumber;

            var shrinkPages = (overflowSize - reducedOverflowSize) / Constants.Storage.PageSize;

            Assert.Equal(allocatedPagesCount - shrinkPages + 1 /* + 1 because free space handling allocated one page during shrink */,
                Env.ScratchBufferPool._current.File.AllocatedPagesCount);

            var pageFromScratchBuffers = tx.LowLevelTransaction.GetTransactionPages();

            shrinkedPageInScratch = pageFromScratchBuffers.First(x => x.NumberOfPages == 3); //after shrinking

            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.FreePage(shrinkedPageNumber);
        }

        Env.FlushLogToDataFile();

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.ModifyPage(0);

            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            tx.LowLevelTransaction.ModifyPage(0);

            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            var overflowSize = 4 * Constants.Storage.PageSize;
            var page = tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);

            var pageFromScratchBuffers = tx.LowLevelTransaction.GetTransactionPages();

            // this page should be taken from freed pages available in scratch file

            var overflowScratchPage = pageFromScratchBuffers.First(x => x.NumberOfPages == 5); // overflow is 4 pages + header so 5 pages in total

            Assert.Equal(shrinkedPageInScratch.PositionInScratchBuffer, overflowScratchPage.PositionInScratchBuffer);
        }
    }
}
