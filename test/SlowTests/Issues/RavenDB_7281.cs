using System;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Voron;
using Voron.Data.Fixed;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7281 : StorageTest
    {
        [Theory]
        [InlineData(13, 2)]
        [InlineData(5111, 2)]
        [InlineData(13, 3)]
        [InlineData(5111, 3)]
        [InlineData(13, 5)]
        [InlineData(5111, 5)]
        public void CanCalculateNumberOfEntriesInFst(int total, int mod)
        {
            DoWork(total, mod, modZero: true);
            DoWork(total, mod, modZero: false);
        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void CanCalculateNumberOfEntriesInFst_Random(int seed)
        {
            var random = new Random(seed);
            var total = random.Next(0, 10000);
            var mod = random.Next(2, 100);

            DoWork(total, mod, modZero: true);
            DoWork(total, mod, modZero: false);
        }

        private void DoWork(int total, int mod, bool modZero)
        {
            var inserted = 0;
            var treeName = Guid.NewGuid().ToString("N");

            using (var txw = Env.WriteTransaction())
            {
                Slice treeNameSlice;
                Slice.From(Allocator, treeName, ByteStringType.Immutable, out treeNameSlice);

                var tree = txw.GetGlobalFixedSizeTree(treeNameSlice, sizeof(long));

                for (var i = 0; i < total; i++)
                {
                    var modResult = i % mod;

                    if (modZero && modResult == 0)
                        continue;

                    if (modZero == false && modResult != 0)
                        continue;

                    tree.Add(i, new byte[sizeof(long)]);
                    inserted++;
                }

                txw.Commit();
            }

            using (var txr = Env.ReadTransaction())
            {
                var tree = txr.FixedTreeFor(treeName);

                for (var i = 0; i < total; i++)
                {
                    var count = tree.GetNumberOfEntriesAfter(i, out long totalCount);
                    var expectedCount = Calculate(tree, i, out long expectedTotalCount);

                    Assert.Equal(inserted, expectedTotalCount);
                    Assert.Equal(expectedTotalCount, totalCount);
                    Assert.Equal(expectedCount, count);
                }
            }
        }

        private static long Calculate(FixedSizeTree fst, long afterValue, out long totalCount)
        {
            totalCount = fst.NumberOfEntries;
            if (totalCount == 0)
                return 0;

            long count = 0;
            using (var it = fst.Iterate())
            {
                if (it.Seek(afterValue) == false)
                    return 0;

                do
                {
                    if (it.CurrentKey == afterValue)
                        continue;

                    count++;
                } while (it.MoveNext());
            }

            return count;
        }
    }
}
