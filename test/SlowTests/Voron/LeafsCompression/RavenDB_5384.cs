using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.LeafsCompression
{
    public class RavenDB_5384 : StorageTest
    {
        [Theory]
        [InlineData(777, 2048, false, 1)]
        [InlineData(777, 2048, false, 2019845912)]
        [InlineData(8192, 512, true, 1)]
        [InlineData(16384, 512, false, 1)]
        [InlineData(26, 512, true, 1)]
        [InlineData(1024, 512, false, 1)]
        [InlineData(26, 333, true, 1)]
        [InlineData(1024, 555, false, 1)]
        [InlineData(254, 713, false, 1104905703)]
        [InlineDataWithRandomSeed(312, 345, true)]
        [InlineDataWithRandomSeed(254, 713, false)]

        public void Leafs_compressed_CRUD(int iterationCount, int size, bool sequentialKeys, int seed)
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

        private void AssertReads(Transaction tx, IEnumerable<string> ids, byte[] bytes)
        {
            var tree = tx.ReadTree("tree");

            foreach (var id in ids)
            {
                Slice key;

                using (Slice.From(tx.Allocator, id, ByteStringType.Immutable, out key))
                {
                    using (var readResult = tree.ReadDecompressed(key))
                    {
                        if (readResult == null)
                        {

                        }

                        var result = readResult.Reader.ReadBytes(readResult.Reader.Length).ToArray();

                        Assert.Equal(bytes, result);
                    }
                }
            }
        }

        private void AssertDeletes(Transaction tx, IEnumerable<string> deleted)
        {
            var tree = tx.ReadTree("tree");

            foreach (var id in deleted)
            {
                Slice key;
                using (Slice.From(tx.Allocator, id, ByteStringType.Immutable, out key))
                {
                    using (var readResult = tree.ReadDecompressed(key))
                    {
                        Assert.Null(readResult);
                    }
                }
            }
        }
    }
}
