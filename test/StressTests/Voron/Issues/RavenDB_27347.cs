using System;
using System.Collections.Generic;
using FastTests.Voron;
using Sparrow.Binary;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Issues
{
    public class RavenDB_27347 : StorageTest
    {
        public RavenDB_27347(ITestOutputHelper output) : base(output)
        {
        }

        // the access pattern of a map-reduce results tree: 8 bytes big endian ids (a new entry is always the greatest key), deletes
        // and in-place updates of recent entries (compression tombstones in the leaf that keeps receiving entries), values of mixed
        // compressibility with a large share of overflow values. The compressed leaves keep getting decompressed for writing,
        // recompressed, split or written back, which is where RavenDB-27347 lived: a failed recompression must not apply the
        // compression tombstones twice, a decompressed page that consumed tombstones must be written back even if it cannot be
        // compressed, and splitting such a page must handle an overflow entry in the middle. The seeds are fixed, the runs are deterministic.
        [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        [InlineData(27347)]
        [InlineData(27348)]
        [InlineData(27349)]
        public void CompressedLeavesSurviveSequentialChurnWithDeletesAndOverflowValues(int seed)
        {
            const int numberOfBatches = 3000;
            const int operationsPerBatch = 50;

            var random = new Random(seed);
            var live = new List<long>();
            var expected = new Dictionary<long, byte[]>();
            var recentlyDeleted = new Queue<long>();
            long nextId = 1;

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);
                tx.Commit();
            }

            for (int batch = 0; batch < numberOfBatches; batch++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");

                    for (int i = 0; i < operationsPerBatch; i++)
                    {
                        var operation = random.Next(100);

                        if (operation < 60 || live.Count < 200)
                        {
                            var value = RandomValue(random);
                            Add(tx, tree, nextId, value);
                            expected[nextId] = value;
                            live.Add(nextId++);
                        }
                        else if (operation < 70)
                        {
                            // a recent entry, most likely already in the compressed section of the hot leaf
                            var id = live[live.Count - 1 - random.Next(5, Math.Min(80, live.Count))];
                            var value = RandomValue(random);
                            Add(tx, tree, id, value);
                            expected[id] = value;
                        }
                        else
                        {
                            // mostly recent entries, sometimes the oldest ones
                            var index = random.Next(10) < 7
                                ? live.Count - 1 - random.Next(5, Math.Min(80, live.Count))
                                : random.Next(Math.Min(100, live.Count));

                            var id = live[index];
                            Delete(tx, tree, id);
                            expected.Remove(id);
                            live.RemoveAt(index);

                            recentlyDeleted.Enqueue(id);

                            if (recentlyDeleted.Count > 200)
                                recentlyDeleted.Dequeue();
                        }
                    }

                    tx.Commit();
                }
            }

            // the tree can be validated and its header compared with a structure walk only once every compressed leaf has consumed its
            // deferred work: decompress each of them for writing and write it back, or take it out of the tree if all of its entries got
            // deleted (what the map-reduce code does with such leaves)
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                var compressedLeaves = new List<long>();

                foreach (var pageNumber in tree.AllPages())
                {
                    var page = GetPage(tx, pageNumber);

                    if (page.IsOverflow == false && page.IsLeaf && page.IsCompressed)
                        compressedLeaves.Add(pageNumber);
                }

                foreach (var pageNumber in compressedLeaves)
                {
                    using (var decompressed = tree.DecompressPage(tree.ModifyPage(pageNumber), DecompressionUsage.Write, skipCache: true))
                    {
                        if (decompressed.NumberOfEntries == 0)
                            tree.RemoveEmptyDecompressedPage(decompressed);
                        else
                            decompressed.CopyToOriginal(tx.LowLevelTransaction, defragRequired: true, wasModified: true, tree);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                tree.ValidateTree_Forced(tree.State.Header.RootPageNumber);

                // AllPages does not see the overflow pages referenced from the compressed sections, the overflow pages are counted here
                long entries = 0;
                long branchPages = 0;
                long leafPages = 0;
                long overflowPages = 0;

                foreach (var pageNumber in tree.AllPages())
                {
                    var page = GetPage(tx, pageNumber);

                    if (page.IsOverflow)
                        continue;

                    if (page.IsBranch)
                    {
                        branchPages++;
                        continue;
                    }

                    leafPages++;

                    if (page.IsCompressed)
                        Assert.Equal(0, page.NumberOfEntries); // written back: nothing left in the uncompressed section, in particular no tombstone

                    using (page.IsCompressed ? (DecompressedLeafPage)(page = tree.DecompressPage(page, DecompressionUsage.Read, skipCache: true)) : null)
                    {
                        entries += page.NumberOfEntries;
                        overflowPages += CountOverflowPages(tx, page);
                    }
                }

                Assert.Equal(expected.Count, entries);
                Assert.Equal(branchPages, tree.State.Header.BranchPages);
                Assert.Equal(leafPages, tree.State.Header.LeafPages);

                // not asserted yet: NumberOfEntries, OverflowPages and PageCount in the header drift because of pre-existing accounting gaps
                // (a delete of an uncompressed key skips the decrement, an update of a compressed key leaks its old overflow page), to be
                // fixed and asserted separately
                // Assert.Equal(expected.Count, tree.State.Header.NumberOfEntries);
                // Assert.Equal(overflowPages, tree.State.Header.OverflowPages);
                // Assert.Equal(branchPages + leafPages + overflowPages, tree.State.Header.PageCount);

                foreach (var (id, value) in expected)
                    Assert.Equal(value, Read(tx, tree, id));

                foreach (var id in recentlyDeleted)
                    Assert.Null(Read(tx, tree, id));
            }
        }

        private static unsafe long CountOverflowPages(Transaction tx, TreePage page)
        {
            long count = 0;

            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var node = page.GetNode(i);

                if (node->Flags == TreeNodeFlags.PageRef)
                    count += VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(GetPage(tx, node->PageNumber).OverflowSize);
            }

            return count;
        }

        private static byte[] RandomValue(Random random)
        {
            var kind = random.Next(100);

            if (kind < 30)
            {
                // overflow value
                var overflow = new byte[random.Next(4200, 6000)];

                if (random.Next(2) == 0)
                    random.NextBytes(overflow);
                else
                    Array.Fill(overflow, (byte)'L');

                return overflow;
            }

            var value = new byte[random.Next(200, 3000)];

            if (kind < 60)
                random.NextBytes(value); // incompressible
            else
                Array.Fill(value, (byte)('a' + random.Next(26))); // compressible

            return value;
        }

        private static unsafe TreePage GetPage(Transaction tx, long pageNumber)
        {
            return new TreePage(tx.LowLevelTransaction.GetPage(pageNumber).Pointer, Constants.Storage.PageSize);
        }

        private static unsafe void Add(Transaction tx, Tree tree, long id, byte[] value)
        {
            id = Bits.SwapBytes(id);

            using (Slice.External(tx.Allocator, (byte*)&id, sizeof(long), out Slice key))
                tree.Add(key, value);
        }

        private static unsafe void Delete(Transaction tx, Tree tree, long id)
        {
            id = Bits.SwapBytes(id);

            using (Slice.External(tx.Allocator, (byte*)&id, sizeof(long), out Slice key))
                tree.Delete(key);
        }

        private static unsafe byte[] Read(Transaction tx, Tree tree, long id)
        {
            id = Bits.SwapBytes(id);

            using (Slice.External(tx.Allocator, (byte*)&id, sizeof(long), out Slice key))
            using (var result = tree.ReadDecompressed(key))
            {
                if (result == null)
                    return null;

                return new ReadOnlySpan<byte>(result.Reader.Base, result.Reader.Length).ToArray();
            }
        }
    }
}
