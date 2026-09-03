using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_27347 : StorageTest
    {
        public RavenDB_27347(ITestOutputHelper output) : base(output)
        {
        }

        // a leaf page has 8192 - 64 = 8128 bytes for nodes. A node takes 11 bytes of header + key + value (padded to an even
        // size) plus a 2 bytes offset. With a 5 bytes key an inline 1000 bytes value takes 1018 bytes, an overflow entry
        // (value of 4053 bytes or more, NodeMaxSize == 4063) is a 18 bytes PageRef node pointing to a separate overflow page.
        private const int OverflowValueSize = 5000;

        private const int InlineValueSize = 1000;

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        public void FailedRecompressionMustNotConsumeCompressionTombstoneTwice()
        {
            var random = new Random(27347);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);

                // two overflow entries, so the double free of the buggy path lands on OverflowPages == 0 instead of tripping
                // Debug.Assert(OverflowPages >= 0) in Tree.RecordFreedPage (which would fail-fast the test host)
                Add(tx, tree, "00000", new byte[OverflowValueSize]);
                Add(tx, tree, "00001", new byte[OverflowValueSize]);

                // 2 x 18 + 7 x 1018 = 7162 bytes, the next 1018 bytes node does not fit anymore
                for (int i = 2; i <= 8; i++)
                    Add(tx, tree, Key(i), new byte[InlineValueSize]);

                // "00009" makes TryCompressPageNodes compress the page for the first time: the zero-filled values compress to
                // almost nothing and the overflow entries land in the compressed section together with them. The incompressible
                // values added afterwards fill the uncompressed section of the compressed leaf.
                for (int i = 9; i <= 15; i++)
                    Add(tx, tree, Key(i), RandomBytes(random, InlineValueSize));

                var root = GetPage(tx, tree.State.Header.RootPageNumber);

                Assert.True(root.IsCompressed);
                Assert.Equal(1, tree.State.Header.Depth);
                Assert.Equal(16, tree.State.Header.NumberOfEntries);
                Assert.Equal(2, tree.State.Header.OverflowPages);
                Assert.Equal(3, tree.State.Header.PageCount);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                // the key lives in the compressed section, so the delete just appends a compression tombstone to the uncompressed
                // section. Decrementing NumberOfEntries and freeing the overflow page is deferred until the page gets decompressed
                // with DecompressionUsage.Write
                Delete(tx, tree, "00000");

                var root = GetPage(tx, tree.State.Header.RootPageNumber);

                Assert.True(root.IsCompressed);
                Assert.True(HasCompressionTombstone(root));
                Assert.Equal(16, tree.State.Header.NumberOfEntries);
                Assert.Equal(2, tree.State.Header.OverflowPages);

                // does not fit into the uncompressed section. TryCompressPageNodes decompresses the page with Write usage, which
                // consumes the tombstone (NumberOfEntries--, overflow page freed), but the incompressible values leave no room for
                // the new entry after the recompression, so the attempt is abandoned and the page gets split instead. The split
                // must not decompress the page again - that would consume the tombstone a second time.
                Add(tx, tree, "000041", RandomBytes(random, 1500));

                Assert.Equal(16, tree.State.Header.NumberOfEntries);
                Assert.Equal(1, tree.State.Header.OverflowPages);
                Assert.Equal(2, tree.State.Header.Depth);
                Assert.Equal(tree.AllPages().Count, tree.State.Header.PageCount);

                tree.ValidateTree_Forced(tree.State.Header.RootPageNumber);

                Assert.Null(ReadLength(tx, tree, "00000"));
                Assert.Equal(OverflowValueSize, ReadLength(tx, tree, "00001"));
                Assert.Equal(1500, ReadLength(tx, tree, "000041"));

                Assert.Equal(16, CountEntries(tree));

                tx.Commit();
            }
        }

        private static string Key(int i)
        {
            return i.ToString("D5");
        }

        private static byte[] RandomBytes(Random random, int size)
        {
            var bytes = new byte[size];
            random.NextBytes(bytes);
            return bytes;
        }

        private static unsafe TreePage GetPage(Transaction tx, long pageNumber)
        {
            return new TreePage(tx.LowLevelTransaction.GetPage(pageNumber).Pointer, Constants.Storage.PageSize);
        }

        private static void Add(Transaction tx, Tree tree, string key, byte[] value)
        {
            using (Slice.From(tx.Allocator, key, out Slice keySlice))
                tree.Add(keySlice, value);
        }

        private static void Delete(Transaction tx, Tree tree, string key)
        {
            using (Slice.From(tx.Allocator, key, out Slice keySlice))
                tree.Delete(keySlice);
        }

        private static int? ReadLength(Transaction tx, Tree tree, string key)
        {
            using (Slice.From(tx.Allocator, key, out Slice keySlice))
            using (var result = tree.ReadDecompressed(keySlice))
                return result?.Reader.Length;
        }

        private static int CountEntries(Tree tree)
        {
            var count = 0;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return 0;

                do
                {
                    count++;
                } while (it.MoveNext());
            }

            return count;
        }

        private static unsafe bool HasCompressionTombstone(TreePage page)
        {
            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                if (page.GetNode(i)->Flags == TreeNodeFlags.CompressionTombstone)
                    return true;
            }

            return false;
        }
    }
}
