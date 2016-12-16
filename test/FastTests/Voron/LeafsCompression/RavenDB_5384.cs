using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Xunit;

namespace FastTests.Voron.LeafsCompression
{
    public class RavenDB_5384 : StorageTest
    {
        // TODO arek - slow test cases move to SlowTests
        [Theory]
        [InlineData(26, 256, true, 1)]
        [InlineData(1024, 256, false, 1)]
        [InlineData(8192, 512, true, 1)]
        [InlineData(16384, 512, false, 1)] 
        [InlineData(26, 333, true, 1)]
        [InlineData(1024, 555, false, 1)]
        [InlineData(777, 2048, false, 1)]
        public void Can_compress_leaf_pages_and_read_directly_from_them_after_decompression(int iterationCount, int size, bool sequentialKeys, int seed)
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree", flags: TreeFlags.LeafsCompressed);
                
                tx.Commit();
            }

            HashSet<string> ids;
            
            var random = new Random(seed);

            if (sequentialKeys)
            {
                ids = new HashSet<string>(Enumerable.Range(0, iterationCount).Select(x => $"{x:d5}"));
            }
            else
            {
                ids = new HashSet<string>();

                while (ids.Count < iterationCount)
                {
                    ids.Add(random.Next().ToString());
                }
            }

            var bytes = new byte[size];
            random.NextBytes(bytes);

            // insert
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                foreach (var id in ids)
                {
                    tree.Add(id, new MemoryStream(bytes));

                    AssertReads(tx, new[] { id }, bytes);
                }

                var compressedLeafs =
                    tree.AllPages()
                        .Select(x => tree.GetReadOnlyTreePage(x))
                        .Where(p => p.Flags.HasFlag(PageFlags.Compressed))
                        .ToList();

                Assert.NotEmpty(compressedLeafs);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                AssertReads(tx, ids, bytes);
            }

            // update
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                foreach (var id in ids)
                {
                    tree.Add(id, new MemoryStream(bytes));
                }

                var compressedLeafs =
                    tree.AllPages()
                        .Select(x => tree.GetReadOnlyTreePage(x))
                        .Where(p => p.Flags.HasFlag(PageFlags.Compressed))
                        .ToList();

                Assert.NotEmpty(compressedLeafs);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                AssertReads(tx, ids, bytes);
            }

            // deletes - partial
            var deleted = new HashSet<string>();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                foreach (var id in ids)
                {
                    if (random.Next() % 2 == 0)
                    {
                        tree.Delete(id);

                        deleted.Add(id);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                AssertReads(tx, ids.Except(deleted), bytes);
                AssertDeletes(tx, deleted);
            }

            // deletes - everything

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                Assert.True(tree.State.Flags.HasFlag(TreeFlags.LeafsCompressed));

                foreach (var id in ids)
                {
                    tree.Delete(id);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                AssertDeletes(tx, ids);
            }
        }

        private unsafe void AssertReads(Transaction tx, IEnumerable<string> ids, byte[] bytes)
        {
            var tree = tx.ReadTree("tree");

            foreach (var id in ids)
            {
                Slice key;
                byte[] result;

                using (Slice.From(tx.Allocator, id, ByteStringType.Immutable, out key))
                {
                    unsafe
                    {
                        TreeNodeHeader* node;
                        Func<Slice, TreeCursor> cursor;
                        var page = tree.FindPageFor(key, out node, out cursor, allowCompressed: true);

                        ReadResult readResult;

                        if (page.IsCompressed)
                        {
                            using (var decompressed = tree.DecompressPage(page))
                            {
                                var nodeNumber = decompressed.NodePositionFor(tx.LowLevelTransaction, key);
                                node = decompressed.GetNode(nodeNumber);

                                readResult = new ReadResult(tree.GetValueReaderFromHeader(node), node->Version);
                            }
                        }
                        else
                        {
                            readResult = new ReadResult(tree.GetValueReaderFromHeader(node), node->Version);
                        }

                        result = readResult.Reader.ReadBytes(readResult.Reader.Length).ToArray();
                    }
                }

                Assert.Equal(bytes, result);
            }
        }

        private unsafe void AssertDeletes(Transaction tx, IEnumerable<string> deleted)
        {
            var tree = tx.ReadTree("tree");

            foreach (var id in deleted)
            {
                Slice key;
                using (Slice.From(tx.Allocator, id, ByteStringType.Immutable, out key))
                {
                    TreeNodeHeader* node;
                    Func<Slice, TreeCursor> cursor;
                    var page = tree.FindPageFor(key, out node, out cursor, allowCompressed: true);

                    if (page.IsCompressed)
                    {
                        using (var decompressed = tree.DecompressPage(page))
                        {
                            decompressed.Search(tx.LowLevelTransaction, key);
                            
                            Assert.NotEqual(0, decompressed.LastMatch);
                        }
                    }
                    else
                    {
                        Assert.NotEqual(0, page.LastMatch);
                    }
                }
            }
        }
    }
}