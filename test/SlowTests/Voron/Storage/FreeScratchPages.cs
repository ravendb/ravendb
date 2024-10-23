// -----------------------------------------------------------------------
//  <copyright file="FreeScratchPages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using Tests.Infrastructure;
using Voron.Impl.Scratch;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class FreeScratchPages : FastTests.Voron.StorageTest
    {
        public FreeScratchPages(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void UncommittedTransactionShouldFreeScratchPagesThatWillBeReusedFutureTransactions()
        {
            var random = new Random();
            var buffer = new byte[1024];
            random.NextBytes(buffer);
            Options.ManualFlushing = true;

            PageFromScratchBuffer[] scratchPagesOfUncommittedTransaction;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                scratchPagesOfUncommittedTransaction = tx.LowLevelTransaction.GetTransactionPages().ToArray();

                // tx.Commit() - intentionally not committing
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(1);
                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(1);
                tx.Commit();
            }

            PageFromScratchBuffer[] scratchPagesOfCommittedTransaction;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                // let's do exactly the same, it should reuse the same scratch pages
                for (int i = 0; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                scratchPagesOfCommittedTransaction = tx.LowLevelTransaction.GetTransactionPages().ToArray();

                tx.Commit();
            }

            Assert.Equal(scratchPagesOfUncommittedTransaction.Length, scratchPagesOfCommittedTransaction.Length);

            foreach (var uncommittedPage in scratchPagesOfUncommittedTransaction)
            {
                Assert.True(
                    scratchPagesOfCommittedTransaction.Any(p => p.File == uncommittedPage.File && p.PositionInScratchBuffer == uncommittedPage.PositionInScratchBuffer)
                );
            }
        }
    }
}
