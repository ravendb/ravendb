using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25424(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void CanHandleTransactionReleasingPagesWhileFlushing()
        {
            Options.ManualFlushing = true;

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
            Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFile_AfterSparseRegionsSet_BeforeWritingToDataFile += () =>
            {
                if (alreadyRun)
                    return;
                alreadyRun = true;
                // this will be ignored, because we _already_ process spare regions
                using (var tx = Env.WriteTransaction())
                {
                    for (int i = 0; i < AllocationSize; i++)
                    {
                        tx.LowLevelTransaction.FreePage(pageNum+i);
                    }
                    tx.Commit();
                }

                // this will be flushed to disk, since we haven't gotten to it yet
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

            // second run, forcing the sparse pages to be zeroed
            Env.FlushLogToDataFile();

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
