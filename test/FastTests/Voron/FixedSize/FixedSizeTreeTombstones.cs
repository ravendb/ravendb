using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Platform;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Voron.Data.Fixed;
using Xunit;
using Constants = Voron.Global.Constants;

namespace FastTests.Voron.FixedSize
{
    public unsafe class FixedSizeTreeTombstones(ITestOutputHelper output) : StorageTest(output)
    {
        internal const ushort ValueSize = 8;

        private static readonly int EntriesPerPage = GetEntriesPerPage(ValueSize);

        internal static int GetEntriesPerPage(ushort valueSize)
        {
            return FixedSizeTreePage<long>.GetTombstonesLayout(Constants.Storage.PageSize, valueSize + sizeof(long)).Capacity;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void DeletingAndAddingTheSameKeyReusesTheTombstone()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var count = EntriesPerPage * 4;
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                var key = EntriesPerPage + 7L;

                Assert.Equal(1L, fst.Delete(key).NumberOfEntriesDeleted);
                Assert.False(fst.Contains(key));
                Assert.True(fst.ReadPtr(key, out _) == null);
                Assert.Equal(count - 1L, fst.NumberOfEntries);

                // the key is still physically in the page, adding it back has to report it as a new entry
                Assert.True(fst.Add(key, key * 3));
                Assert.Equal(key * 3, ReadValue(fst, key));

                // and the resurrected entry is a regular one, so writing it again is an update
                Assert.False(fst.Add(key, key));

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, Enumerable.Range(0, count).Select(x => (long)x));
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(-1)]
        [InlineData(1)]
        public void ATombstoneIsReusedByANeighbouringKey(int offset)
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var keys = new List<long>();
                for (int i = 0; i < EntriesPerPage * 4; i++)
                {
                    // leave gaps so that we have keys to insert between the existing ones
                    var key = i * 10L;
                    fst.Add(key, key);
                    keys.Add(key);
                }

                var deleted = EntriesPerPage * 10L + 30;
                Assert.Equal(1L, fst.Delete(deleted).NumberOfEntriesDeleted);
                keys.Remove(deleted);

                // the new key sorts between the neighbours of the tombstone, so it can take over its slot
                var added = deleted + offset;
                Assert.True(fst.Add(added, added));
                keys.Add(added);
                keys.Sort();

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, keys);
                Assert.False(fst.Contains(deleted));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AFullPageIsCompactedInsteadOfSplitWhenItHasTombstones()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var expected = new List<long>();
                for (int i = 0; i < EntriesPerPage; i++)
                {
                    fst.Add(i, (long)i);
                    expected.Add(i);
                }

                Assert.Equal(1L, fst.PageCount);

                // free up a few slots in the middle of the full page
                for (int i = 100; i < 110; i++)
                {
                    Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                    expected.Remove(i);
                }

                // appending now does not fit in the page as it stands, but it does once the tombstones are gone
                for (int i = EntriesPerPage; i < EntriesPerPage + 10; i++)
                {
                    Assert.True(fst.Add(i, (long)i));
                    expected.Add(i);
                }

                Assert.Equal(1L, fst.PageCount);
                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected);

                // there are no tombstones left to reclaim, so this one has to split
                var last = EntriesPerPage + 10L;
                Assert.True(fst.Add(last, last));
                expected.Add(last);

                Assert.True(fst.PageCount > 1);
                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void PagesThatAreMostlyTombstonesAreMergedOnRebalance()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var count = EntriesPerPage * 10;
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                var pagesWhenFull = fst.PageCount;
                var expected = new List<long>();

                // keep one entry out of every twenty, so every page falls well below the merge threshold
                for (int i = 0; i < count; i++)
                {
                    if (i % 20 == 0)
                    {
                        expected.Add(i);
                        continue;
                    }

                    Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                }

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected);
                Assert.True(fst.PageCount < pagesWhenFull, $"Expected the tree to shrink from {pagesWhenFull} pages, but it has {fst.PageCount}");
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void IteratorsSkipDeletedEntries()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var count = EntriesPerPage * 5;
                var expected = new List<long>();
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                for (int i = 0; i < count; i++)
                {
                    // delete in runs of three, so iterators have to skip more than one entry at a time,
                    // including at the very start of the tree
                    if (i % 7 < 3)
                    {
                        Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                        continue;
                    }

                    expected.Add(i);
                }

                AssertAllEntries(fst, expected);

                using (var it = fst.Iterate())
                {
                    // seeking to a deleted key lands on the first live key after it
                    Assert.True(it.Seek(0));
                    Assert.Equal(expected[0], it.CurrentKey);

                    Assert.True(it.Seek(7));
                    Assert.Equal(expected.First(x => x >= 7), it.CurrentKey);

                    Assert.True(it.SeekBackward(9));
                    Assert.Equal(expected.Last(x => x <= 9), it.CurrentKey);

                    Assert.True(it.SeekToLast());
                    Assert.Equal(expected[^1], it.CurrentKey);
                }

                // walking backwards from the end has to see the same entries
                var backwards = new List<long>();
                using (var it = fst.Iterate())
                {
                    Assert.True(it.SeekToLast());
                    do
                    {
                        backwards.Add(it.CurrentKey);
                    } while (it.MovePrev());
                }

                backwards.Reverse();
                Assert.Equal(expected, backwards);

                foreach (var skip in new[] { 0, 1, 5, 17, 100, expected.Count - 1 })
                {
                    using (var it = fst.Iterate())
                    {
                        Assert.True(it.Seek(long.MinValue));
                        Assert.True(it.Skip(skip));
                        Assert.Equal(expected[skip], it.CurrentKey);

                        Assert.True(it.Skip(-skip));
                        Assert.Equal(expected[0], it.CurrentKey);
                    }
                }

                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(long.MinValue));
                    Assert.False(it.Skip(expected.Count));
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void NumberOfEntriesAfterIgnoresDeletedEntries()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var count = EntriesPerPage * 6;
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                var expected = new List<long>();
                for (int i = 0; i < count; i++)
                {
                    if (i % 3 == 0)
                    {
                        Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                        continue;
                    }

                    expected.Add(i);
                }

                var after = fst.GetNumberOfEntriesAfter(expected[0], out var totalCount, Stopwatch.StartNew(), EstimationAccuracy.Exact);

                Assert.Equal((long)expected.Count, totalCount);
                Assert.Equal(expected.Count - 1L, after);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void PagesInTheClassicLayoutAreConvertedOnDelete()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var count = EntriesPerPage * 4;
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                Assert.Equal(0, CountPagesWithTombstones(tx, fst));

                var expected = new List<long>();
                for (int i = 0; i < count; i++)
                {
                    if (i % 2 == 0)
                    {
                        Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                        continue;
                    }

                    expected.Add(i);
                }

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected);
                Assert.True(CountPagesWithTombstones(tx, fst) > 0, "Deleting from the pages should have converted them");
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AClassicPageThatHoldsMoreEntriesThanFitNextToABitmapIsSplitFirst()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var expected = new List<long>();
                for (int i = 0; i < EntriesPerPage; i++)
                {
                    fst.Add(i, (long)i);
                    expected.Add(i);
                }

                // this version stops filling a leaf at the count that leaves room for a bitmap, so the only
                // way to get a fuller page is the way an older version would have written it
                var classicEntriesPerPage = (Constants.Storage.PageSize - Constants.FixedSizeTree.PageHeaderSize) / (ValueSize + sizeof(long));
                Assert.True(classicEntriesPerPage > EntriesPerPage);

                AppendEntriesAsAnOlderVersionWould(tx, fst, EntriesPerPage, classicEntriesPerPage - EntriesPerPage);

                for (int i = EntriesPerPage; i < classicEntriesPerPage; i++)
                {
                    expected.Add(i);
                }

                Assert.Equal(1L, fst.PageCount);
                Assert.Equal(0, CountPagesWithTombstones(tx, fst));

                // there is no room for a bitmap in this page, so it has to be split before it can be converted
                var deleted = EntriesPerPage / 2L;
                Assert.Equal(1L, fst.Delete(deleted).NumberOfEntriesDeleted);
                expected.Remove(deleted);

                Assert.True(fst.PageCount > 1);
                Assert.True(CountPagesWithTombstones(tx, fst) > 0);

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ClassicAndConvertedPagesCanLiveInTheSameTree()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                var count = EntriesPerPage * 10;
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                var expected = new HashSet<long>();

                // only the pages holding the lower half of the keys get converted, the rest stay classic
                for (int i = 0; i < count; i++)
                {
                    if (i < count / 2 && i % 3 == 0)
                    {
                        Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                        continue;
                    }

                    expected.Add(i);
                }

                var withTombstones = CountPagesWithTombstones(tx, fst);
                Assert.True(withTombstones > 0);
                Assert.True(withTombstones < fst.PageCount);

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected.OrderBy(x => x));

                // and now force the two kinds of pages to merge into each other
                for (int i = 0; i < count; i++)
                {
                    if (expected.Contains(i) == false || i % 5 == 0)
                        continue;

                    Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                    expected.Remove(i);
                }

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected.OrderBy(x => x));
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(0, 8)]
        [InlineData(1337, 8)]
        [InlineData(7919, 8)]
        [InlineData(982451, 0)]
        [InlineData(104729, 0)]
        [InlineData(6857, 1008)]
        [InlineData(15485863, 1008)]
        [InlineData(2038, 40)]
        public void RandomAddDeleteAndReAddMatchesAReferenceModel(int seed, ushort valueSize)
        {
            var random = new System.Random(seed);
            var reference = new SortedSet<long>();
            var keyRange = GetEntriesPerPage(valueSize) * 12;

            using (var tx = Env.WriteTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, valueSize);

                for (int i = 0; i < 60_000; i++)
                {
                    var key = random.Next(keyRange);

                    if (random.Next(10) < 5)
                        Assert.Equal(reference.Remove(key) ? 1L : 0L, fst.Delete(key).NumberOfEntriesDeleted);
                    else
                        Assert.Equal(reference.Add(key), Add(fst, key, valueSize));

                    Assert.Equal((long)reference.Count, fst.NumberOfEntries);
                }

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, reference, valueSize);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TombstonedPagesSurviveARestart()
        {
            RequireFileBasedPager();

            var expected = FillAndDelete(Env, Allocator, ValueSize, EntriesPerPage * 6);

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, ValueSize);

                fst.ValidateTree_Forced();
                AssertAllEntries(fst, expected);
            }
        }

        internal static List<long> FillAndDelete(StorageEnvironment env, ByteStringContext allocator, ushort valueSize, int count)
        {
            var expected = new List<long>();

            using (var tx = env.WriteTransaction())
            using (Slice.From(allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, valueSize);
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, (long)i);
                }

                for (int i = 0; i < count; i++)
                {
                    if (i % 4 == 1)
                    {
                        Assert.Equal(1L, fst.Delete(i).NumberOfEntriesDeleted);
                        continue;
                    }

                    expected.Add(i);
                }

                tx.Commit();
            }

            return expected;
        }

        internal static bool Add(FixedSizeTree fst, long key, ushort valueSize)
        {
            if (valueSize == 0)
                return fst.Add(key);

            if (valueSize == sizeof(long))
                return fst.Add(key, key);

            var value = new byte[valueSize];
            System.BitConverter.TryWriteBytes(value, key);
            return fst.Add(key, value);
        }

        internal static long ReadValue(FixedSizeTree fst, long key, int valueSize = ValueSize)
        {
            var ptr = fst.ReadPtr(key, out var size);
            Assert.True(ptr != null, $"Expected to find a value for {key}");
            Assert.Equal(valueSize, size);
            return *(long*)ptr;
        }

        internal static void AssertAllEntries(FixedSizeTree fst, IEnumerable<long> expectedKeys, ushort valueSize = ValueSize)
        {
            var expected = expectedKeys.ToList();

            Assert.Equal((long)expected.Count, fst.NumberOfEntries);

            var actual = new List<long>();
            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue))
                {
                    do
                    {
                        actual.Add(it.CurrentKey);
                    } while (it.MoveNext());
                }
            }

            Assert.Equal(expected, actual);

            foreach (var key in expected)
            {
                Assert.True(fst.Contains(key), $"Expected to find {key}");

                if (valueSize >= sizeof(long))
                    Assert.Equal(key, ReadValue(fst, key, valueSize));
            }
        }

        /// <summary>
        /// Writes entries straight into the tree's only leaf page, past the point where this version stops
        /// filling one, so we get a page in the shape an older version of the format would have left behind.
        /// </summary>
        private static void AppendEntriesAsAnOlderVersionWould(Transaction tx, FixedSizeTree fst, long firstKey, int count)
        {
            var pages = fst.AllPages();
            Assert.Equal(1, pages.Count);

            var page = GetPage(tx, fst, pages[0], writable: true);
            Assert.True(page.IsLeaf);
            Assert.False(page.HasTombstonesBitmap);
            Assert.Equal(Constants.FixedSizeTree.PageHeaderSize, page.StartPosition);

            var entrySize = fst.ValueSize + sizeof(long);
            for (int i = 0; i < count; i++)
            {
                var entry = page.Pointer + page.StartPosition + ((page.NumberOfEntries + i) * entrySize);
                *(long*)entry = firstKey + i;
                *(long*)(entry + sizeof(long)) = firstKey + i;
            }

            page.NumberOfEntries += (ushort)count;

            using (fst.Parent.DirectAdd(fst.Name, sizeof(FixedSizeTreeHeader.Large), out byte* header))
            {
                ((FixedSizeTreeHeader.Large*)header)->NumberOfEntries += count;
            }
        }

        internal static int CountPagesWithTombstones(Transaction tx, FixedSizeTree fst)
        {
            var count = 0;
            foreach (var pageNumber in fst.AllPages())
            {
                if (GetPage(tx, fst, pageNumber, writable: false).HasTombstonesBitmap)
                    count++;
            }

            return count;
        }

        private static FixedSizeTreePage<long> GetPage(Transaction tx, FixedSizeTree fst, long pageNumber, bool writable)
        {
            var llt = tx.LowLevelTransaction;
            var pointer = writable ? llt.ModifyPage(pageNumber).Pointer : llt.GetPage(pageNumber).Pointer;

            return new FixedSizeTreePage<long>(pointer, fst.ValueSize + sizeof(long), Constants.Storage.PageSize);
        }
    }

    public class FixedSizeTreeTombstonesEncrypted(ITestOutputHelper output) : StorageTest(output)
    {
        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            options.Encryption.MasterKey = _masterKey.ToArray();
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void TombstonedPagesSurviveARestartOfAnEncryptedEnvironment()
        {
            RequireFileBasedPager();

            const ushort valueSize = 8;
            var count = FixedSizeTreeTombstones.GetEntriesPerPage(valueSize) * 6;

            var expected = FixedSizeTreeTombstones.FillAndDelete(Env, Allocator, valueSize, count);

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            using (Slice.From(Allocator, "test", out var name))
            {
                var fst = tx.FixedTreeFor(name, valueSize);

                fst.ValidateTree_Forced();
                FixedSizeTreeTombstones.AssertAllEntries(fst, expected);
            }
        }
    }
}
