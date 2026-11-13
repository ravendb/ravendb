using System.Linq;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Xunit;

namespace FastTests.Voron.PostingLists
{
    public unsafe class RavenDB_27934(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void PageMergesDoNotKeepTheirBuffersUntilTheTransactionEnds()
        {
            const int lists = 8;
            var ids = Enumerable.Range(1, 375_000).Select(i => (long)i << 10).ToArray();
            // removing 4 of every 5 runs of 5,000 neighbours leaves the leaf pages under half full
            var removals = ids.Where((_, i) => i / 5_000 % 5 != 0).Select(x => x | 1).ToArray();
            var states = new PostingListState[lists];

            using (var wtx = Env.WriteTransaction())
            {
                for (int i = 0; i < lists; i++)
                {
                    PostingList.Create(wtx.LowLevelTransaction, ref states[i]);
                    Update(wtx.LowLevelTransaction, ref states[i], ids, []);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                int leafPagesBefore = states.Sum(x => x.LeafPages);
                long before = NativeMemory.CurrentThreadStats.TotalAllocated;

                for (int i = 0; i < lists; i++)
                    Update(wtx.LowLevelTransaction, ref states[i], [], removals);

                long held = NativeMemory.CurrentThreadStats.TotalAllocated - before;
                int merges = leafPagesBefore - states.Sum(x => x.LeafPages);
                Output.WriteLine($"{merges} merges, {held / 1024:N0} KB held by the transaction");

                Assert.True(merges >= 32, $"expected many merges, got {merges}");
                // each merge used to hold about 1 MB until the transaction ended, now the memory is reused
                Assert.True(held < 20 * 1024 * 1024, $"{held / 1024:N0} KB held for {merges} merges");
            }
        }

        private static void Update(LowLevelTransaction llt, ref PostingListState state, long[] additions, long[] removals)
        {
            using var encoder = new FastPForEncoder(llt.Allocator);
            var decoder = new FastPForDecoder(llt.Allocator);
            var tempList = new ContextBoundNativeList<long>(llt.Allocator);
            try
            {
                fixed (long* a = additions)
                fixed (long* r = removals)
                    PostingList.Update(llt, ref state, a, additions.Length, r, removals.Length, encoder, ref tempList, ref decoder);
            }
            finally
            {
                tempList.Dispose();
                decoder.Dispose();
            }
        }
    }
}
