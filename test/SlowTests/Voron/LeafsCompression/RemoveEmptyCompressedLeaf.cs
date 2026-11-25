using System;
using System.Collections.Generic;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Xunit;

namespace SlowTests.Voron.LeafsCompression
{
    public unsafe class RemoveEmptyCompressedLeaf(ITestOutputHelper output) : StorageTest(output)
    {
        private const string TreeName = "tree";
        private const int NumberOfEntries = 5_000;

        // A full compressed leaf has about 50 bytes free, so deleting its first keys decompresses, removes and compresses it again.
        // That frees enough space for the rest of the deletes to be compression tombstones, so in the end the leaf is still in the
        // tree, compressed, and decompresses to no entries. Map-reduce takes such leaves out of the tree with RemoveEmptyDecompressedPage.
        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Compression)]
        public void LeafEmptiedByCompressionTombstonesIsRemovedFromTheTree()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(TreeName, flags: TreeFlags.LeafsCompressed);
                var value = new byte[128];
                for (int i = 0; i < NumberOfEntries; i++)
                {
                    value.AsSpan().Fill((byte)'v'); // compresses well, so the leaves get compressed
                    BitConverter.TryWriteBytes(value, i);
                    using (Slice.From(tx.Allocator, Key(i), out Slice key))
                    using (Slice.From(tx.Allocator, value, out Slice slice))
                        tree.Add(key, slice);
                }

                tx.Commit();
            }

            long emptiedLeaf;
            var deletedKeys = new HashSet<string>();
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                var compressedLeaves = CompressedLeaves(tree);
                Assert.True(compressedLeaves.Count >= 3, $"expected at least 3 compressed leaves, got {compressedLeaves.Count}");

                emptiedLeaf = compressedLeaves[compressedLeaves.Count / 2]; // neither the first nor the last leaf
                using (var decompressed = tree.DecompressPage(tree.GetReadOnlyTreePage(emptiedLeaf), DecompressionUsage.Read, skipCache: true))
                {
                    for (int i = 0; i < decompressed.NumberOfEntries; i++)
                    {
                        using (TreeNodeHeader.ToSlicePtr(tx.Allocator, decompressed.GetNode(i), out Slice key))
                            deletedKeys.Add(key.ToString());
                    }
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                foreach (var key in deletedKeys)
                {
                    using (Slice.From(tx.Allocator, key, out Slice slice))
                        tree.Delete(slice);
                }

                var page = tree.GetReadOnlyTreePage(emptiedLeaf);
                Assert.True(tree.AllPages().Contains(emptiedLeaf) && page.IsLeaf && page.IsCompressed, $"page {emptiedLeaf} is no longer a compressed leaf of the tree");

                using (var decompressed = tree.DecompressPage(page, DecompressionUsage.Read, skipCache: true))
                {
                    Assert.Equal(0, decompressed.NumberOfEntries);

                    tree.RemoveEmptyDecompressedPage(decompressed);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree(TreeName);
                Assert.DoesNotContain(emptiedLeaf, tree.AllPages());

                tree.ValidateTree_Forced(tree.State.Header.RootPageNumber);

                long leafPages = 0;
                foreach (var pageNumber in tree.AllPages())
                {
                    if (tree.GetReadOnlyTreePage(pageNumber).IsLeaf)
                        leafPages++;
                }
                Assert.Equal(leafPages, tree.State.Header.LeafPages);

                var found = 0;
                using (var it = tree.Iterate(prefetch: false))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            Assert.DoesNotContain(it.CurrentKey.ToString(), deletedKeys);
                            found++;
                        } while (it.MoveNext());
                    }
                }
                Assert.Equal(NumberOfEntries - deletedKeys.Count, found);

                for (int i = 0; i < NumberOfEntries; i++)
                {
                    using (Slice.From(tx.Allocator, Key(i), out Slice key))
                    using (var result = tree.ReadDecompressed(key))
                        Assert.True(deletedKeys.Contains(Key(i)) == (result == null), $"key {Key(i)}: deleted {deletedKeys.Contains(Key(i))}, found {result != null}");
                }
            }
        }

        private static string Key(int i) => $"k{i:D6}";

        private static List<long> CompressedLeaves(Tree tree)
        {
            var leaves = new List<long>();
            foreach (var pageNumber in tree.AllPages())
            {
                var page = tree.GetReadOnlyTreePage(pageNumber);
                if (page.IsLeaf && page.IsCompressed)
                    leaves.Add(pageNumber);
            }

            return leaves;
        }
    }
}
