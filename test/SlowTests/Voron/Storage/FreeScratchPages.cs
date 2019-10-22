// -----------------------------------------------------------------------
//  <copyright file="FreeScratchPages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
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

        [Fact]
        public void UncommittedTransactionShouldFreeScratchPagesThatWillBeReusedByNextTransaction()
        {
            var random = new Random();
            var buffer = new byte[1024];
            random.NextBytes(buffer);

            HashSet<PageFromScratchBuffer> scratchPagesOfUncommittedTransaction;

            using (var tx = Env.WriteTransaction())
            {
                 var tree = tx.CreateTree("foo");
               for (int i = 0; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                scratchPagesOfUncommittedTransaction = new HashSet<PageFromScratchBuffer>(tx.LowLevelTransaction.GetTransactionPages());

                // tx.Commit() - intentionally not committing
            }

            HashSet<PageFromScratchBuffer> scratchPagesOfCommittedTransaction;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                // let's do exactly the same, it should reuse the same scratch pages
                for (int i = 0; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                scratchPagesOfCommittedTransaction = new HashSet<PageFromScratchBuffer>(tx.LowLevelTransaction.GetTransactionPages());

                tx.Commit();
            }

            Assert.Equal(scratchPagesOfUncommittedTransaction.Count, scratchPagesOfCommittedTransaction.Count);

            foreach (var uncommittedPage in scratchPagesOfUncommittedTransaction)
            {
                Assert.Contains(uncommittedPage, scratchPagesOfCommittedTransaction);
            }
        }
    }
}
