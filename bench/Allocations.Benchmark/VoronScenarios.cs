using System;
using Sparrow.Binary;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;

namespace Allocations.Benchmark
{
    // Voron reads and writes on a memory-only environment, a new one for every scenario
    public unsafe class VoronScenarios : IDisposable
    {
        private const int NumberOfKeys = 100_000;
        private const int Stride = 997; // prime, so consecutive operations land on different leaf pages

        private static readonly Slice DocsSlice, AllEtagsSlice, CollectionEtagsSlice;

        private readonly StorageEnvironment _env = new(StorageEnvironmentOptions.CreateMemoryOnlyForTests());

        static VoronScenarios()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out DocsSlice);
                Slice.From(ctx, "AllEtags", ByteStringType.Immutable, out AllEtagsSlice);
                Slice.From(ctx, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
            }
        }

        public void Trees()
        {
            using (var tx = _env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"k{i:D8}", out Slice key))
                        tree.Add(key, (long)i);
                }

                tx.Commit();
            }

            using (var tx = _env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                var i = 0;
                Measure.Report("Tree.TryRead, scattered", Measure.PerOperation(() => tree.TryRead(keys[i++ % NumberOfKeys], out _)));

                using (var it = tree.Iterate(prefetch: false))
                {
                    var i1 = 0;
                    Measure.Report("TreeIterator.Seek, scattered", Measure.PerOperation(() => it.Seek(keys[i1++ % NumberOfKeys])));
                }

                var i2 = 0;
                Measure.Report("Tree iterator + Seek, reused", Measure.PerOperation(() =>
                {
                    using (var it = tree.Iterate(prefetch: false))
                        it.Seek(keys[i2++ % NumberOfKeys]);
                }));
            }

            using (var tx = _env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                for (int j = 0; j < NumberOfKeys; j++)
                    tree.Add(keys[j], (long)j);

                var i1 = 0;
                Measure.Report("Tree.Add update, scattered", Measure.PerOperation(() => tree.Add(keys[i1++ % NumberOfKeys], (long)i1)));

                var sequential = CreateKeys(tx, "k{0:D8}", stride: 1);
                var i2 = 0;
                Measure.Report("Tree.Add update, sequential", Measure.PerOperation(() => tree.Add(sequential[i2++ % NumberOfKeys], (long)i2)));

                var appended = CreateKeys(tx, "z{0:D8}", stride: 1);
                var i3 = 0;
                Measure.Report("Tree.Add new key, appended", Measure.PerOperation(() => tree.Add(appended[i3++ % NumberOfKeys], (long)i3)));

                var i4 = 0;
                Measure.Report("Tree.Delete, scattered", Measure.PerOperation(() => tree.Delete(keys[i4++ % NumberOfKeys])));
            }
        }

        public void TreesWithCompressedLeaves()
        {
            using (var tx = _env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo", flags: TreeFlags.LeafsCompressed);
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"k{i:D8}", out Slice key))
                        tree.Add(key, (long)i);
                }

                tx.Commit();
            }

            using (var tx = _env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                for (int j = 0; j < NumberOfKeys; j++)
                    tree.Add(keys[j], (long)j);

                var i1 = 0;
                Measure.Report("Compressed Tree.Add update", Measure.PerOperation(() => tree.Add(keys[i1++ % NumberOfKeys], (long)i1)));

                var i2 = 0;
                Measure.Report("Compressed Tree.TryReadDecompressed", Measure.PerOperation(() =>
                {
                    if (tree.TryReadDecompressed(keys[i2++ % NumberOfKeys], out var value))
                        value.Dispose();
                }));
            }
        }

        public void FixedSizeTrees()
        {
            using (var tx = _env.WriteTransaction())
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

            using (var tx = _env.ReadTransaction())
            {
                var fst = tx.ReadTree("parent").FixedTreeFor(AllEtagsSlice, valSize: 8);

                long i = 0;
                Measure.Report("FST.ReadPtr, scattered", Measure.PerOperation(() => fst.ReadPtr((i++ * Stride % NumberOfKeys) * 2, out _)));
            }

            using (var tx = _env.WriteTransaction())
            {
                var fst = tx.ReadTree("parent").FixedTreeFor(AllEtagsSlice, valSize: 8);

                long i1 = 0;
                Measure.Report("FST.Add update, scattered", Measure.PerOperation(() => fst.Add((i1++ * Stride % NumberOfKeys) * 2, i1)));

                long i2 = NumberOfKeys * 2;
                Measure.Report("FST.Add new key, appended", Measure.PerOperation(() => fst.Add(i2++, i2)));

                long i3 = 0;
                Measure.Report("FST.Delete, scattered", Measure.PerOperation(() => fst.Delete((i3++ * Stride % NumberOfKeys) * 2)));
            }
        }

        public void Tables()
        {
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { StartIndex = 1, Name = AllEtagsSlice, IsGlobal = true })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { StartIndex = 1, Name = CollectionEtagsSlice });

            var payload = new byte[128];
            var builder = new TableValueBuilder();
            long etag = 0;

            using (var tx = _env.WriteTransaction())
            {
                schema.Create(tx, DocsSlice, 16);
                var table = tx.OpenTable(schema, DocsSlice);
                var ids = CreateKeys(tx, "docs/{0:D8}", stride: 1);
                for (int i = 0; i < NumberOfKeys; i++)
                    Put(table, builder, ids[i], ++etag, payload);

                tx.Commit();
            }

            using (var tx = _env.WriteTransaction())
            {
                var table = tx.OpenTable(schema, DocsSlice);
                var scattered = CreateStridedKeys(tx, "docs/{0:D8}");

                var i1 = 0;
                Measure.Report("Table.Set update, scattered", Measure.PerOperation(() => Put(table, builder, scattered[i1++ % NumberOfKeys], ++etag, payload)));

                var appended = CreateKeys(tx, "docs/new/{0:D8}", stride: 1);
                var i2 = 0;
                Measure.Report("Table.Set insert, appended", Measure.PerOperation(() => Put(table, builder, appended[i2++ % NumberOfKeys], ++etag, payload)));

                var etagIndex = schema.FixedSizeIndexes[AllEtagsSlice];
                long e1 = 0;
                Measure.Report("Table.SeekForwardFrom (fixed), 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekForwardFrom(etagIndex, Bits.SwapBytes(++e1 * 997 % NumberOfKeys), 0))
                        break;
                }));

                long e2 = 0;
                Measure.Report("Table.SeekBackwardFrom (fixed), 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekBackwardFrom(etagIndex, Bits.SwapBytes(++e2 * 997 % NumberOfKeys)))
                        break;
                }));

                var i4 = 0;
                Measure.Report("Table.SeekByPrimaryKeyPrefix, 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekByPrimaryKeyPrefix(scattered[i4++ % NumberOfKeys], Slices.Empty, 0))
                        break;
                }));

                var i3 = 0;
                Measure.Report("Table.DeleteByKey, scattered", Measure.PerOperation(() => table.DeleteByKey(scattered[i3++ % NumberOfKeys])));
            }
        }

        public void TableSeeks()
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

            using (var tx = _env.WriteTransaction())
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

            using (var tx = _env.ReadTransaction())
            {
                var table = tx.OpenTable(schema, DocsSlice);
                var ids = CreateStridedKeys(tx, "docs/{0:D8}");
                var groups = new Slice[numberOfGroups];
                for (int i = 0; i < numberOfGroups; i++)
                    Slice.From(tx.Allocator, $"group/{i * 997L % numberOfGroups:D8}", ByteStringType.Immutable, out groups[i]);
                Slice.From(tx.Allocator, "docs/", ByteStringType.Immutable, out var docsPrefix);

                var i1 = 0;
                Measure.Report("Table.SeekOneForwardFromPrefix", Measure.PerOperation(() =>
                    table.SeekOneForwardFromPrefix(groupIndex, groups[i1++ % numberOfGroups], out _)));

                var i2 = 0;
                Measure.Report("Table.SeekOneBackwardFrom", Measure.PerOperation(() =>
                    table.SeekOneBackwardFrom(groupIndex, groups[i2 % numberOfGroups], groups[i2++ % numberOfGroups], out _)));

                var i3 = 0;
                Measure.Report("Table.SeekOnePrimaryKeyPrefix, after", Measure.PerOperation(() =>
                    table.SeekOnePrimaryKeyPrefix(docsPrefix, ids[i3++ % NumberOfKeys], out _)));

                var i4 = 0;
                Measure.Report("Table.SeekForwardFrom (index), 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekForwardFrom(groupIndex, groups[i4++ % numberOfGroups], 0, startsWith: true))
                        break;
                }));

                var i5 = 0;
                Measure.Report("Table.SeekByPrimaryKey, 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekByPrimaryKey(ids[i5++ % NumberOfKeys], 0))
                        break;
                }));

                var i6 = 0;
                Measure.Report("Table.SeekForwardFromPrefix, 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekForwardFromPrefix(groupIndex, groups[i6 % numberOfGroups], groups[i6++ % numberOfGroups], 0))
                        break;
                }));

                var i7 = 0;
                Measure.Report("Table.SeekBackwardFrom (index), 1st", Measure.PerOperation(() =>
                {
                    foreach (var _ in table.SeekBackwardFrom(groupIndex, groups[i7 % numberOfGroups], groups[i7++ % numberOfGroups]))
                        break;
                }));
            }
        }

        // every row has its own index value, like the IdAndEtag index of revisions
        public void TableUniqueIndex()
        {
            Slice uniqueSlice;
            using (StorageEnvironment.GetStaticContext(out var ctx))
                Slice.From(ctx, "Unique", ByteStringType.Immutable, out uniqueSlice);

            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineIndex(new TableSchema.IndexDef { StartIndex = 1, Count = 1, Name = uniqueSlice });

            var payload = new byte[64];
            var builder = new TableValueBuilder();

            using (var tx = _env.WriteTransaction())
            {
                schema.Create(tx, DocsSlice, 16);
                tx.Commit();
            }

            using (var tx = _env.WriteTransaction())
            {
                var table = tx.OpenTable(schema, DocsSlice);
                var ids = CreateKeys(tx, "docs/{0:D8}", stride: 1);
                var values = CreateKeys(tx, "value/{0:D8}", stride: 1);

                var i1 = 0;
                Measure.Report("Table.Set insert, unique index value", Measure.PerOperation(() =>
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
                Measure.Report("Table.DeleteByKey, unique index value", Measure.PerOperation(() => table.DeleteByKey(ids[i2++])));
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

        private static Slice[] CreateStridedKeys(global::Voron.Impl.Transaction tx, string format) => CreateKeys(tx, format, Stride);

        private static Slice[] CreateKeys(global::Voron.Impl.Transaction tx, string format, int stride)
        {
            var keys = new Slice[NumberOfKeys];
            for (int i = 0; i < NumberOfKeys; i++)
                Slice.From(tx.Allocator, string.Format(format, (long)i * stride % NumberOfKeys), ByteStringType.Immutable, out keys[i]);
            return keys;
        }

        public void Dispose() => _env.Dispose();
    }
}
