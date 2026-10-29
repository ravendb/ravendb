using System;
using System.IO;
using System.Linq;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Impl.Compaction;
using Xunit;

namespace FastTests.Voron.Compaction
{
    // Compaction copies the trees of a table's global indexes only when it is not going to rebuild them, and
    // rebuilds them by re-inserting the rows. A global dynamic key index has to be on that skip list like the
    // other global ones: copied and rebuilt both, the second write of the same key throws.
    public unsafe class RavenDB_22932_DynamicKeyIndexes : StorageTest
    {
        private const string BucketIndexedTable = "RavenDB_22932_IndexedByBucket";

        private static readonly Slice IndexName;
        private static readonly Slice BucketIndexName;

        static RavenDB_22932_DynamicKeyIndexes()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "RavenDB_22932_GlobalDynamicKeyIndex", ByteStringType.Immutable, out IndexName);
                Slice.From(ctx, "RavenDB_22932_BucketIndex", ByteStringType.Immutable, out BucketIndexName);
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

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(DataDir)))
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

            StorageCompaction.Execute(StorageEnvironmentOptions.ForPathForTests(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPathForTests(CompactedDir));

            using (var compacted = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(CompactedDir)))
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

                // the index entries hold the row ids of the compacted table, so a tree copied verbatim
                // from the source would resolve to rows that are not there
                var index = (TableSchema.DynamicKeyIndexDef)schema.DynamicKeyIndexes[IndexName];
                var etagsFromIndex = table.SeekByPrefix(index, Slices.Empty, Slices.Empty, skip: 0)
                    .Select(result => result.Result.Reader.ReadLong(1))
                    .ToList();

                Assert.Equal(Enumerable.Range(0, entries).Where(i => i % 2 == 1).Select(i => (long)i), etagsFromIndex);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CompactingATableIndexedByBucketIntoAPlainEnvironmentNamesTheTable()
        {
            var schema = CreateBucketIndexedSchema();

            using (var env = new StorageEnvironment(ShardedOptionsFor(DataDir)))
            using (var tx = env.WriteTransaction())
            {
                schema.Create(tx, BucketIndexedTable, 16);
                var table = tx.OpenTable(schema, BucketIndexedTable);

                using (Slice.From(tx.Allocator, "users/1", out var key))
                {
                    table.Set(new TableValueBuilder { key, 1L });
                }

                tx.Commit();
            }

            // the destination carries no sharding configuration, so rebuilding the index is what fails
            var error = Assert.Throws<InvalidOperationException>(() => StorageCompaction.Execute(
                ShardedOptionsFor(DataDir),
                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPathForTests(CompactedDir)));

            Assert.Contains(BucketIndexedTable, error.Message);
            Assert.Contains("was not opened for a shard", error.Message);
        }

        private static StorageEnvironmentOptions ShardedOptionsFor(string path)
        {
            return new ShardedDirectoryStorageEnvironmentOptions(path, tempPath: null, journalPath: null,
                ioChangesNotifications: null, catastrophicFailureNotification: null,
                loggingResource: null, configuration: new ShardingConfiguration());
        }

        private static TableSchema CreateBucketIndexedSchema()
        {
            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1, IsGlobal = false });

            schema.DefineIndex(new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = GenerateBucketKey,
                IsGlobal = true,
                Name = BucketIndexName
            });

            return schema;
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketKey(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            var idPtr = tvr.Read(0, out var idSize);
            var bucket = ShardedDocumentsStorage.GetBucketFor(tx, new ReadOnlySpan<byte>(idPtr, idSize));

            var scope = tx.Allocator.Allocate(sizeof(int), out var buffer);
            *(int*)buffer.Ptr = Bits.SwapBytes(bucket);

            slice = new Slice(buffer);
            return scope;
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
    }
}
