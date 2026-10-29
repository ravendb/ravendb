using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using static Raven.Server.Documents.Schemas.Attachments;

namespace SlowTests.Issues
{
    public class RavenDB_22932 : RavenTestBase
    {
        public RavenDB_22932(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanCompactDatabaseOrShardWithData(Options options, bool withAttachment)
        {
            options.RunInMemory = false; // compaction requires on-disk storage

            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";
                var content = new byte[] { 1, 2, 3, 4 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, id);

                    if (withAttachment)
                        session.Advanced.Attachments.Store(id, "file.bin", new MemoryStream(content), "application/octet-stream");

                    await session.SaveChangesAsync();
                }

                // a sharded database must be compacted shard by shard, so compact the shard that holds the document
                var databaseName = options.DatabaseMode == RavenDatabaseMode.Sharded
                    ? ShardHelper.ToShardName(store.Database, await Sharding.GetShardNumberForAsync(store, id))
                    : store.Database;

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = databaseName,
                    Documents = true
                }));

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    if (withAttachment)
                    {
                        using (var attachment = await session.Advanced.Attachments.GetAsync(id, "file.bin"))
                        using (var stream = new MemoryStream())
                        {
                            Assert.NotNull(attachment);
                            await attachment.Stream.CopyToAsync(stream);
                            Assert.Equal(content, stream.ToArray());
                        }
                    }
                }

                if (options.DatabaseMode != RavenDatabaseMode.Sharded)
                    return;

                // loading by id does not read the bucket index - the bucket is recomputed on the orchestrator and the
                // shard then reads by primary key - so a wrong bucket written during compaction would go unnoticed.
                // these read through the index the compaction rebuilt.
                var bucket = await Sharding.GetBucketAsync(store, id);
                var shard = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(databaseName);

                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var documents = shard.ShardedDocumentsStorage.GetDocumentsByBucketFrom(ctx, bucket, etag: 0).ToList();
                    Assert.Equal(id, Assert.Single(documents).Id);

                    if (withAttachment)
                    {
                        var attachments = shard.DocumentsStorage.AttachmentsStorage
                            .GetAttachmentsByBucketFrom(ctx, bucket, etag: 0, remoteAttachments: false).ToList();

                        Assert.Equal("file.bin", Assert.Single(attachments).Name);
                    }
                }
            }
        }

        // Without a prefix setting the bucket is the hash of the id and nothing else, so it comes out the same
        // whether compaction was handed the database's sharding configuration, an empty one, or none at all -
        // the test above cannot tell them apart. A prefix adds BucketRangeStart to that hash, and then only the
        // right configuration puts the rebuilt index entries where the document is looked for.
        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanCompactAShardOfAPrefixedShardedDatabase()
        {
            const string id = "eu/1";
            var content = new byte[] { 1, 2, 3, 4 };

            using (var store = Sharding.GetDocumentStore(new Options
            {
                RunInMemory = false,
                ModifyDatabaseRecord = record =>
                {
                    record.Sharding ??= new ShardingConfiguration();
                    record.Sharding.Prefixed = [new PrefixedShardingSetting { Prefix = "eu/", Shards = [0] }];
                }
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, id);
                    session.Advanced.Attachments.Store(id, "file.bin", new MemoryStream(content), "application/octet-stream");

                    await session.SaveChangesAsync();
                }

                var bucket = await Sharding.GetBucketAsync(store, id);

                // a prefixed bucket lives past the plain hash range, which is what the configuration adds
                Assert.True(bucket >= 1024 * 1024, $"'{id}' landed in bucket {bucket}, so the prefix was not applied and the test proves nothing");

                var shardName = ShardHelper.ToShardName(store.Database, await Sharding.GetShardNumberForAsync(store, id));

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = shardName,
                    Documents = true
                }));

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var shard = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(shardName);

                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var documents = shard.ShardedDocumentsStorage.GetDocumentsByBucketFrom(ctx, bucket, etag: 0).ToList();
                    Assert.Equal(id, Assert.Single(documents).Id);

                    var attachments = shard.DocumentsStorage.AttachmentsStorage
                        .GetAttachmentsByBucketFrom(ctx, bucket, etag: 0, remoteAttachments: false).ToList();

                    Assert.Equal("file.bin", Assert.Single(attachments).Name);
                }
            }
        }

        // AttachmentsFlagAndHash is a global dynamic key index on the attachments schema of every database,
        // sharded or not, and it supports duplicate keys - so copying its tree verbatim and rebuilding it
        // leaves the stale ids next to the new ones instead of throwing.
        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CompactionLeavesOneEntryPerAttachmentInTheFlagAndHashIndex()
        {
            const string id = "users/1";

            using (var store = GetDocumentStore(new Options { RunInMemory = false }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, id);
                    session.Advanced.Attachments.Store(id, "file.bin", new MemoryStream([1, 2, 3, 4]), "application/octet-stream");

                    await session.SaveChangesAsync();
                }

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }));

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var database = await GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tx = ctx.Transaction.InnerTransaction;
                    var schema = database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema;
                    var index = schema.DynamicKeyIndexes[AttachmentsFlagAndHashSlice];
                    var table = tx.OpenTable(schema, AttachmentsMetadataSlice);

                    var indexTree = tx.ReadTree(AttachmentsFlagAndHashSlice, isIndexTree: true);
                    Assert.NotNull(indexTree);

                    using (var it = indexTree.Iterate(prefetch: false))
                    {
                        Assert.True(it.Seek(Slices.BeforeAllKeys), "the flag and hash index is empty after the compaction");

                        do
                        {
                            Assert.Equal(1, table.GetCountOfMatchesFor(index, it.CurrentKey));
                        } while (it.MoveNext());
                    }
                }
            }
        }

        // Creating a database over existing data goes through AdminDatabasesHandler.RecreateDatabase, which opens each
        // shard before the record reaches the cluster - so before FillPrefixedSharding has given the prefixes their
        // BucketRangeStart. The zip is shard 0 of a 6.2 database with Prefix = "eu/", holding document eu/1 and an
        // attachment, so opening it runs From62000, which recomputes the attachments' bucket index keys.
        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanCreateAPrefixedShardedDatabaseOverShardDataFrom62()
        {
            const string id = "eu/1";

            var zipPath = new PathSetting("SchemaUpgrade/Issues/DocumentsVersion/RavenDB_22932_PrefixedShard_62.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            var name = GetDatabaseName();
            var shardName = ShardHelper.ToShardName(name, 0);
            var record = new DatabaseRecord(name)
            {
                Settings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                },
                Sharding = new ShardingConfiguration
                {
                    Shards = new Dictionary<int, DatabaseTopology>
                    {
                        { 0, new DatabaseTopology() },
                        { 1, new DatabaseTopology() },
                        { 2, new DatabaseTopology() }
                    },
                    Orchestrator = new OrchestratorConfiguration
                    {
                        Topology = new OrchestratorTopology()
                    },
                    Prefixed = [new PrefixedShardingSetting { Prefix = "eu/", Shards = [0] }]
                }
            };

            var shardDirectory = DatabasesLandlord.CreateDatabaseConfiguration(Server.ServerStore, shardName, record.Settings).Core.DataDirectory.FullPath;
            ZipFile.ExtractToDirectory(zipPath.FullPath, shardDirectory);

            using (var store = new DocumentStore { Urls = [Server.WebUrl], Database = name }.Initialize())
            {
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(record));

                var bucket = await Sharding.GetBucketAsync(store, id);
                Assert.True(bucket >= ShardHelper.NumberOfBuckets, $"'{id}' landed in bucket {bucket}, so the prefix was not applied and the test proves nothing");

                var shard = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(shardName);

                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var documents = shard.ShardedDocumentsStorage.GetDocumentsByBucketFrom(ctx, bucket, etag: 0).ToList();
                    Assert.Equal(id, Assert.Single(documents).Id);

                    var attachments = shard.DocumentsStorage.AttachmentsStorage
                        .GetAttachmentsByBucketFrom(ctx, bucket, etag: 0, remoteAttachments: false).ToList();
                    Assert.Equal("file.bin", Assert.Single(attachments).Name);
                }

                // created by hand, so the test infrastructure would delete the files of a database that is still loaded
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(name, hardDelete: true));
                Assert.True(await WaitForValueAsync(() => Enumerable.Range(0, record.Sharding.Shards.Count)
                    .All(shardNumber => Server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(ShardHelper.ToShardName(name, shardNumber)) == false), true));
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
