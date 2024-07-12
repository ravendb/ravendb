using FastTests.Voron;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_15302 : StorageTest
    {
        public RavenDB_15302(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AfterCopyToOriginalDecompressedPageShouldNotBeInCacheIfCompressionWasNotNeeded()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                TreePage treePage = tree.NewPage(TreePageFlags.Leaf, 0);

                var decompressed = tree.GetDecompressedPage(16 * 1024, DecompressionUsage.Write, treePage);

                tree.DecompressionsCache.Add(decompressed);

                decompressed.CopyToOriginal(tx.LowLevelTransaction, false, true, tree);

                Assert.False(tree.DecompressionsCache.TryGet(treePage.PageNumber, DecompressionUsage.Write, out _));
            }
        }
    }
}
