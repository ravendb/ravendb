using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Tests.Infrastructure;
using Voron.Impl.Scratch;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class FreeScratchPages : FastTests.Voron.StorageTest
    {
        public FreeScratchPages(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void UncommittedTransactionShouldFreeScratchPagesThatWillBeReusedByNextTransaction()
        {
            var random = new Random();
            var buffer = new byte[1024];
            random.NextBytes(buffer);

            HashSet<(int FileNumber, long Position, long Size, int NumberOfPages)> scratchPagesOfUncommittedTransaction;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                scratchPagesOfUncommittedTransaction = ScratchPositionsOf(tx);

                // tx.Commit() - intentionally not committing
            }

            HashSet<(int FileNumber, long Position, long Size, int NumberOfPages)> scratchPagesOfCommittedTransaction;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                // let's do exactly the same, it should reuse the same scratch pages
                for (int i = 0; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                scratchPagesOfCommittedTransaction = ScratchPositionsOf(tx);

                tx.Commit();
            }

            Assert.Equal(scratchPagesOfUncommittedTransaction.Count, scratchPagesOfCommittedTransaction.Count);

            foreach (var uncommittedPage in scratchPagesOfUncommittedTransaction)
            {
                Assert.Contains(uncommittedPage, scratchPagesOfCommittedTransaction);
            }
        }

        private static HashSet<(int FileNumber, long Position, long Size, int NumberOfPages)> ScratchPositionsOf(global::Voron.Impl.Transaction tx)
        {
            return tx.LowLevelTransaction.GetTransactionPages()
                .Select(p => (p.File.Number, p.PositionInScratchBuffer, p.Size, p.NumberOfPages))
                .ToHashSet();
        }
    }
}
