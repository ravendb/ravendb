using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25424(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void CanHandleTransactionReleasingPagesWhileFlushing()
        {
            Options.ManualFlushing = true;
            Options.ManualSyncing = true;

            const int AllocationSize= 600;
            const int OverflowSize = AllocationSize * Voron.Global.Constants.Storage.PageSize - Voron.PageHeader.SizeOf;

            long pageNum;
            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(AllocationSize);
                p.OverflowSize = OverflowSize;
                p.Flags |= Voron.PageFlags.Overflow;
                pageNum = p.PageNumber;
                p.AsSpan(Voron.PageHeader.SizeOf, OverflowSize).Fill(1);
                tx.Commit();
            }

            bool alreadyRun = false;
            Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFile_BeforeWritingToDataFile += () =>
            {
                if (alreadyRun)
                    return;
                alreadyRun = true;
                // committed while the flush is running - the next flush consumes this free and registers its sparse region for the deferred punch
                using (var tx = Env.WriteTransaction())
                {
                    for (int i = 0; i < AllocationSize; i++)
                    {
                        tx.LowLevelTransaction.FreePage(pageNum+i);
                    }
                    tx.Commit();
                }

                // the reallocation is written by the next flush, which also subtracts these pages from the pending sparse regions
                using (var tx = Env.WriteTransaction())
                {
                    var p = tx.LowLevelTransaction.AllocatePage(AllocationSize);
                    Assert.Equal(pageNum, p.PageNumber);
                    p.OverflowSize = OverflowSize;
                    p.Flags |= Voron.PageFlags.Overflow;
                    p.AsSpan(Voron.PageHeader.SizeOf, OverflowSize).Fill(2);
                    tx.Commit();
                }
            };

            // first run, to create the "race" with the transactions
            Env.FlushLogToDataFile();

            // second run flushes the free + reallocation
            Env.FlushLogToDataFile();

            // punch whatever is still pending - it must not touch the reallocated pages
            using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                Assert.True(sync.SyncDataFile());

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.GetPage(pageNum);
                pageNum = p.PageNumber;
                Assert.Equal(OverflowSize, p.OverflowSize);
                var span = p.AsSpan(Voron.PageHeader.SizeOf, OverflowSize);
                Assert.False(span.ContainsAnyExcept((byte)2));
            }
        }
    }
}
