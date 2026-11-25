using System;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class LeafPageNumber(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void TheLeafPageNumberIsTheLeafThatHoldsTheKey()
        {
            Slice.From(Allocator, "etags", out Slice treeId);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                for (long i = 1; i <= 100_000; i++)
                    fst.Add(i * 3, new byte[8]);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                Assert.True(fst.Depth >= 2);
                var random = new Random(1);
                for (var i = 0; i < 1_000; i++)
                {
                    var key = 3 * random.NextInt64(1, 100_001);
                    Assert.True(fst.TryGetLeafPageNumber(key, out var pageNumber));
                    var page = fst.GetReadOnlyPage(pageNumber);
                    Assert.True(page.IsLeaf);
                    var found = false;
                    for (var entry = 0; entry < page.NumberOfEntries && found == false; entry++)
                        found = page.GetKey(entry) == key;
                    Assert.True(found, $"key {key} is not in leaf page {pageNumber}");
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ASinglePageTreeHasNoLeafToRead()
        {
            Slice.From(Allocator, "small", out Slice treeId);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                for (long i = 1; i <= 10; i++)
                    fst.Add(i, new byte[8]);
                Assert.False(fst.TryGetLeafPageNumber(5, out _)); // embedded, then a single page: the root is the leaf
                tx.Commit();
            }
        }
    }
}
