using System;
using System.IO;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Impl.Compaction;
using Voron.Util.Settings;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Compaction
{
    // Compaction copies the trees of a table's global indexes only when it is not going to rebuild them, and
    // rebuilds them by re-inserting the rows. A global dynamic key index has to be on that skip list like the
    // other global ones: copied and rebuilt both, the second write of the same key throws.
    public unsafe class RavenDB_22932_DynamicKeyIndexes : StorageTest
    {
        private static readonly Slice IndexName;

        static RavenDB_22932_DynamicKeyIndexes()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "RavenDB_22932_GlobalDynamicKeyIndex", ByteStringType.Immutable, out IndexName);
            }
        }

        public RavenDB_22932_DynamicKeyIndexes(ITestOutputHelper output) : base(output)
        {
            IOExtensions.DeleteDirectory(DataDir);
            IOExtensions.DeleteDirectory(CompactedDir);
        }

        private string CompactedDir => Path.Combine(DataDir, "Compacted");

        [RavenFact(RavenTestCategory.Voron)]
        public void CanCompactATableWithAGlobalDynamicKeyIndex()
        {
            const int entries = 100;

            var schema = CreateSchema();

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    schema.Create(tx, "test", 16);
                    var table = tx.OpenTable(schema, "test");

                    for (var i = 0; i < entries; i++)
                    {
                        using (Slice.From(tx.Allocator, "key" + i, out var key))
                        {
                            table.Set(new TableValueBuilder { key, (long)i });
                        }
                    }

                    tx.Commit();
                }

                // deletions are what makes compaction worth doing, and what leaves the holes it removes
                using (var tx = env.WriteTransaction())
                {
                    var table = tx.OpenTable(schema, "test");

                    for (var i = 0; i < entries; i += 2)
                    {
                        using (Slice.From(tx.Allocator, "key" + i, out var key))
                        {
                            table.DeleteByKey(key);
                        }
                    }

                    tx.Commit();
                }
            }

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPath(DataDir), CompactionOptionsFor(CompactedDir));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPath(CompactedDir)))
            using (var tx = compacted.ReadTransaction())
            {
                var table = tx.OpenTable(schema, "test");

                Assert.Equal(entries / 2, table.NumberOfEntries);

                for (var i = 1; i < entries; i += 2)
                {
                    using (Slice.From(tx.Allocator, "key" + i, out var key))
                    {
                        Assert.True(table.ReadByKey(key, out var reader), $"key{i} did not survive the compaction");
                        Assert.Equal(i, reader.ReadLong(1));
                    }
                }
            }
        }

        private static TableSchema CreateSchema()
        {
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1, IsGlobal = false });

            schema.DefineIndex(new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = GenerateKeyFromEtag,
                IsGlobal = true,
                Name = IndexName
            });

            return schema;
        }

        // a static named method, because the schema persists its type and method name and resolves it by
        // reflection - compaction reads the schema back out of the table tree it is copying
        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateKeyFromEtag(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            var etag = tvr.ReadLong(1);

            var scope = tx.Allocator.Allocate(sizeof(long), out var buffer);
            *(long*)buffer.Ptr = Bits.SwapBytes(etag);

            slice = new Slice(buffer);
            return scope;
        }

        private static StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions CompactionOptionsFor(string path)
        {
            var basePath = new VoronPathSetting(path);

            return new RebuildingOptions(basePath, new VoronPathSetting(StorageEnvironmentOptions.GetTempPath(path)),
                basePath.Combine("Journals"));
        }

        // the destination has to say that it can rebuild a dynamic key index, the way a shard's options do
        private sealed class RebuildingOptions : StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions
        {
            public RebuildingOptions(VoronPathSetting basePath, VoronPathSetting tempPath, VoronPathSetting journalPath)
                : base(basePath, tempPath, journalPath, null, null)
            {
            }

            public override bool CanRebuildDynamicKeyIndexes => true;
        }
    }
}
