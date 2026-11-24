using System.Collections.Generic;
using System.Linq;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
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

        [RavenFact(RavenTestCategory.Voron)]
        public void AFailedMergeFollowedByAnEmptiedPageKeepsTheListCorrect()
        {
            // TryMerge encodes with the caller's encoder; a merge that does not fit must leave it Done,
            // or the next page that loses all of its entries writes the merged batch into new pages.
            // All values share their low 10 bits, so every page strips them and packs about one bit per entry.
            var values = Enumerable.Range(1, 300_000).Select(k => (long)k << 10).ToArray();
            var state = new PostingListState();

            using (var wtx = Env.WriteTransaction())
            {
                PostingList.Create(wtx.LowLevelTransaction, ref state);
                Update(wtx.LowLevelTransaction, ref state, values, []);
                wtx.Commit();
            }

            List<List<long>> leaves;
            using (var rtx = Env.ReadTransaction())
            {
                leaves = new PostingList(rtx.LowLevelTransaction, Slices.Empty, state).AllPages()
                    .Select(n => rtx.LowLevelTransaction.GetPage(n))
                    .Where(p => ((PostingListLeafPageHeader*)p.Pointer)->PageType == ExtendedPageType.PostingListLeaf)
                    .Select(p => new PostingListLeafPage(p).GetDebugOutput())
                    .OrderBy(x => x[0])
                    .ToList();
            }

            Assert.True(leaves.Count >= 5, $"need at least five leaves, got {leaves.Count}");

            var p = leaves[2]; // keeps a dense tail: small on disk, but about 11 bits per entry without the shared low bits
            var l = leaves[3]; // gets values with other low bits and is emptied below half, so it tries to merge with p and fails
            var n = leaves[4]; // emptied completely: its update encodes nothing
            const int keepFromP = 15_000;
            Assert.True(p.Count > keepFromP, $"leaf 2 has only {p.Count} entries");

            var removals = new SortedSet<long>();
            removals.UnionWith(p.Take(p.Count - keepFromP));
            removals.UnionWith(l.Skip(200));
            removals.UnionWith(n);
            var additions = new SortedSet<long> { l[0] + 4, l[1] + 4, l[2] + 4 };

            using (var wtx = Env.WriteTransaction())
            {
                Update(wtx.LowLevelTransaction, ref state, additions.ToArray(), removals.Select(x => x | 1).ToArray());
                wtx.Commit();
            }

            var expected = new SortedSet<long>(values);
            expected.ExceptWith(removals);
            expected.UnionWith(additions);
            using (var rtx = Env.ReadTransaction())
            {
                var actual = new PostingList(rtx.LowLevelTransaction, Slices.Empty, state).DumpAllValues();
                Assert.Equal(expected.ToList(), actual);
                Assert.Equal(expected.Count, state.NumberOfEntries);
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
