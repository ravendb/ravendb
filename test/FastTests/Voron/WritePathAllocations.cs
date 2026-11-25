using System;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron
{
    public unsafe class WritePathAllocations(ITestOutputHelper output) : StorageTest(output)
    {
        private const int NumberOfKeys = 100_000;
        private const int Stride = 997; // prime, so consecutive operations land on different leaf pages
        private const int NumberOfOperations = 20_000;

        private static readonly Slice DocsSlice, AllEtagsSlice, CollectionEtagsSlice;

        static WritePathAllocations()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out DocsSlice);
                Slice.From(ctx, "AllEtags", ByteStringType.Immutable, out AllEtagsSlice);
                Slice.From(ctx, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Tree_operations()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"k{i:D8}", out Slice key))
                        tree.Add(key, (long)i);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                var i = 0;
                AssertAtMost(8, "Tree.TryRead, scattered", MeasurePerOperation(() => tree.TryRead(keys[i++ % NumberOfKeys], out _)));

                using (var it = tree.Iterate(prefetch: false))
                {
                    var i1 = 0;
                    AssertAtMost(8, "TreeIterator.Seek, scattered", MeasurePerOperation(() => it.Seek(keys[i1++ % NumberOfKeys])));
                }

                var i2 = 0;
                AssertAtMost(8, "Tree iterator + Seek, reused", MeasurePerOperation(() =>
                {
                    using (var it = tree.Iterate(prefetch: false))
                        it.Seek(keys[i2++ % NumberOfKeys]);
                }));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                for (int j = 0; j < NumberOfKeys; j++)
                    tree.Add(keys[j], (long)j);

                var i1 = 0;
                AssertAtMost(8, "Tree.Add update, scattered", MeasurePerOperation(() => tree.Add(keys[i1++ % NumberOfKeys], (long)i1)));

                var sequential = CreateKeys(tx, "k{0:D8}", stride: 1);
                var i2 = 0;
                AssertAtMost(8, "Tree.Add update, sequential", MeasurePerOperation(() => tree.Add(sequential[i2++ % NumberOfKeys], (long)i2)));

                var appended = CreateKeys(tx, "z{0:D8}", stride: 1);
                var i3 = 0;
                AssertAtMost(16, "Tree.Add new key, appended", MeasurePerOperation(() => tree.Add(appended[i3++ % NumberOfKeys], (long)i3)));

                var i4 = 0;
                AssertAtMost(8, "Tree.Delete, scattered", MeasurePerOperation(() => tree.Delete(keys[i4++ % NumberOfKeys])));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Tree_with_compressed_leaves_operations()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo", flags: TreeFlags.LeafsCompressed);
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"k{i:D8}", out Slice key))
                        tree.Add(key, (long)i);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                for (int j = 0; j < NumberOfKeys; j++)
                    tree.Add(keys[j], (long)j);

                var i1 = 0;
                AssertAtMost(8, "Compressed Tree.Add update", MeasurePerOperation(() => tree.Add(keys[i1++ % NumberOfKeys], (long)i1)));

                var i2 = 0;
                AssertAtMost(128, "Compressed Tree.TryReadDecompressed", MeasurePerOperation(() =>
                {
                    if (tree.TryReadDecompressed(keys[i2++ % NumberOfKeys], out var value))
                        value.Dispose();
                }));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void FixedSizeTree_operations()
        {
            using (var tx = Env.WriteTransaction())
            {
                var parent = tx.CreateTree("parent");
                for (int i = 0; i < 10_000; i++)
                {
                    using (Slice.From(tx.Allocator, $"other/{i:D8}", out Slice key))
                        parent.Add(key, (long)i);
                }

                var fst = parent.FixedTreeFor(AllEtagsSlice, valSize: 8);
                for (long i = 0; i < NumberOfKeys; i++)
                    fst.Add(i * 2, i);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.ReadTree("parent").FixedTreeFor(AllEtagsSlice, valSize: 8);

                long i = 0;
                AssertAtMost(8, "FST.ReadPtr, scattered", MeasurePerOperation(() => fst.ReadPtr((i++ * Stride % NumberOfKeys) * 2, out _)));
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.ReadTree("parent").FixedTreeFor(AllEtagsSlice, valSize: 8);

                long i1 = 0;
                AssertAtMost(8, "FST.Add update, scattered", MeasurePerOperation(() => fst.Add((i1++ * Stride % NumberOfKeys) * 2, i1)));

                long i2 = NumberOfKeys * 2;
                AssertAtMost(8, "FST.Add new key, appended", MeasurePerOperation(() => fst.Add(i2++, i2)));

                long i3 = 0;
                AssertAtMost(8, "FST.Delete, scattered", MeasurePerOperation(() => fst.Delete((i3++ * Stride % NumberOfKeys) * 2)));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Table_operations()
        {
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { StartIndex = 1, Name = AllEtagsSlice, IsGlobal = true })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { StartIndex = 1, Name = CollectionEtagsSlice });

            var payload = new byte[128];
            var builder = new TableValueBuilder();
            long etag = 0;

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, DocsSlice, 16);
                var table = tx.OpenTable(schema, DocsSlice);
                var ids = CreateKeys(tx, "docs/{0:D8}", stride: 1);
                for (int i = 0; i < NumberOfKeys; i++)
                    Put(table, builder, ids[i], ++etag, payload);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var table = tx.OpenTable(schema, DocsSlice);
                var scattered = CreateStridedKeys(tx, "docs/{0:D8}");

                var i1 = 0;
                AssertAtMost(32, "Table.Set update, scattered", MeasurePerOperation(() => Put(table, builder, scattered[i1++ % NumberOfKeys], ++etag, payload)));

                var appended = CreateKeys(tx, "docs/new/{0:D8}", stride: 1);
                var i2 = 0;
                AssertAtMost(32, "Table.Set insert, appended", MeasurePerOperation(() => Put(table, builder, appended[i2++ % NumberOfKeys], ++etag, payload)));

                var etagIndex = schema.FixedSizeIndexes[AllEtagsSlice];
                long e1 = 0;
                AssertAtMost(48, "Table.SeekForwardFrom (fixed), 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekForwardFrom(etagIndex, Bits.SwapBytes(++e1 * 997 % NumberOfKeys), 0))
                        break;
                }));

                long e2 = 0;
                AssertAtMost(48, "Table.SeekBackwardFrom (fixed), 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekBackwardFrom(etagIndex, Bits.SwapBytes(++e2 * 997 % NumberOfKeys)))
                        break;
                }));

                var i4 = 0;
                AssertAtMost(64, "Table.SeekByPrimaryKeyPrefix, 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekByPrimaryKeyPrefix(scattered[i4++ % NumberOfKeys], Slices.Empty, 0))
                        break;
                }));

                var i3 = 0;
                AssertAtMost(8, "Table.DeleteByKey, scattered", MeasurePerOperation(() => table.DeleteByKey(scattered[i3++ % NumberOfKeys])));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Table_seek_operations()
        {
            Slice groupsSlice;
            using (StorageEnvironment.GetStaticContext(out var ctx))
                Slice.From(ctx, "Groups", ByteStringType.Immutable, out groupsSlice);

            var groupIndex = new TableSchema.IndexDef { StartIndex = 1, Count = 1, Name = groupsSlice };
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineIndex(groupIndex);

            var payload = new byte[64];
            var builder = new TableValueBuilder();
            const int numberOfGroups = 1_000;

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, DocsSlice, 16);
                var table = tx.OpenTable(schema, DocsSlice);
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"docs/{i:D8}", out Slice id))
                    using (Slice.From(tx.Allocator, $"group/{i % numberOfGroups:D8}", out Slice group))
                    {
                        builder.Reset();
                        builder.Add(id);
                        builder.Add(group);
                        fixed (byte* p = payload)
                        {
                            builder.Add(p, payload.Length);
                            table.Set(builder);
                        }
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var table = tx.OpenTable(schema, DocsSlice);
                var ids = CreateStridedKeys(tx, "docs/{0:D8}");
                var groups = new Slice[numberOfGroups];
                for (int i = 0; i < numberOfGroups; i++)
                    Slice.From(tx.Allocator, $"group/{i * 997L % numberOfGroups:D8}", ByteStringType.Immutable, out groups[i]);
                Slice.From(tx.Allocator, "docs/", ByteStringType.Immutable, out var docsPrefix);

                var i1 = 0;
                AssertAtMost(8, "Table.SeekOneForwardFromPrefix", MeasurePerOperation(() =>
                    table.SeekOneForwardFromPrefix(groupIndex, groups[i1++ % numberOfGroups], out _)));

                var i2 = 0;
                AssertAtMost(8, "Table.SeekOneBackwardFrom", MeasurePerOperation(() =>
                    table.SeekOneBackwardFrom(groupIndex, groups[i2 % numberOfGroups], groups[i2++ % numberOfGroups], out _)));

                var i3 = 0;
                AssertAtMost(8, "Table.SeekOnePrimaryKeyPrefix, after", MeasurePerOperation(() =>
                    table.SeekOnePrimaryKeyPrefix(docsPrefix, ids[i3++ % NumberOfKeys], out _)));

                var i4 = 0;
                AssertAtMost(64, "Table.SeekForwardFrom (index), 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekForwardFrom(groupIndex, groups[i4++ % numberOfGroups], 0, startsWith: true))
                        break;
                }));

                var i5 = 0;
                AssertAtMost(64, "Table.SeekByPrimaryKey, 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekByPrimaryKey(ids[i5++ % NumberOfKeys], 0))
                        break;
                }));

                var i6 = 0;
                AssertAtMost(64, "Table.SeekForwardFromPrefix, 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekForwardFromPrefix(groupIndex, groups[i6 % numberOfGroups], groups[i6++ % numberOfGroups], 0))
                        break;
                }));

                var i7 = 0;
                AssertAtMost(64, "Table.SeekBackwardFrom (index), 1st", MeasurePerOperation(() =>
                {
                    foreach (var _ in table.SeekBackwardFrom(groupIndex, groups[i7 % numberOfGroups], groups[i7++ % numberOfGroups]))
                        break;
                }));
            }
        }

        // every row has its own index value, like the IdAndEtag index of revisions
        [RavenFact(RavenTestCategory.Voron)]
        public void Table_unique_index_operations()
        {
            Slice uniqueSlice;
            using (StorageEnvironment.GetStaticContext(out var ctx))
                Slice.From(ctx, "Unique", ByteStringType.Immutable, out uniqueSlice);

            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineIndex(new TableSchema.IndexDef { StartIndex = 1, Count = 1, Name = uniqueSlice });

            var payload = new byte[64];
            var builder = new TableValueBuilder();

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, DocsSlice, 16);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var table = tx.OpenTable(schema, DocsSlice);
                var ids = CreateKeys(tx, "docs/{0:D8}", stride: 1);
                var values = CreateKeys(tx, "value/{0:D8}", stride: 1);

                var i1 = 0;
                AssertAtMost(64, "Table.Set insert, unique index value", MeasurePerOperation(() =>
                {
                    builder.Reset();
                    builder.Add(ids[i1]);
                    builder.Add(values[i1++]);
                    fixed (byte* p = payload)
                    {
                        builder.Add(p, payload.Length);
                        table.Set(builder);
                    }
                }));

                var i2 = 0;
                AssertAtMost(32, "Table.DeleteByKey, unique index value", MeasurePerOperation(() => table.DeleteByKey(ids[i2++])));
            }
        }

        private static void Put(Table table, TableValueBuilder builder, Slice id, long etag, byte[] payload)
        {
            var bigEndianEtag = Bits.SwapBytes(etag);
            builder.Reset();
            builder.Add(id);
            builder.Add(&bigEndianEtag, sizeof(long));
            fixed (byte* p = payload)
            {
                builder.Add(p, payload.Length);
                table.Set(builder);
            }
        }

        // Before RavenDB-27880 these operations allocated a page object per tree level
        // (and FixedSizeTree operations read their header from the parent tree up to 3 times), 120 - 1,350 bytes per operation.
        private void AssertAtMost(int limit, string name, double bytesPerOperation)
        {
            Output.WriteLine($"{name,-32} {bytesPerOperation,8:F1} bytes/op");
#if !DEBUG // debug-only checks in Voron allocate too
            Assert.True(bytesPerOperation <= limit, $"{name} allocated {bytesPerOperation:F1} bytes/op, expected at most {limit}");
#endif
        }

        private static Slice[] CreateStridedKeys(global::Voron.Impl.Transaction tx, string format) => CreateKeys(tx, format, Stride);

        private static Slice[] CreateKeys(global::Voron.Impl.Transaction tx, string format, int stride)
        {
            var keys = new Slice[NumberOfKeys];
            for (int i = 0; i < NumberOfKeys; i++)
                Slice.From(tx.Allocator, string.Format(format, (long)i * stride % NumberOfKeys), ByteStringType.Immutable, out keys[i]);
            return keys;
        }

        private static double MeasurePerOperation(Action action)
        {
            for (int i = 0; i < 1_000; i++) // warm up so JIT and pool filling are excluded from the measurement
                action();

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < NumberOfOperations; i++)
                action();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfOperations;
        }
    }
}
