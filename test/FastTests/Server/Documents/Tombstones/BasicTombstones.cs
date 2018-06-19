using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Tombstones
{
    public class BasicTombstones : RavenLowLevelTestBase
    {
        [Fact]
        public void CanCreateAndGetTombstone()
        {
            using (var database = CreateDocumentDatabase())
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                DocumentsStorage.PutOperationResults result;
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                    {
                        ["Name"] = "John",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Users"
                        }
                    }))
                    {
                        result = database.DocumentsStorage.Put(context, "key/1", null, doc);
                    }

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    Assert.NotNull(database.DocumentsStorage.Delete(context, "key/1", null));

                    tx.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var tombstones = database
                        .DocumentsStorage
                        .GetTombstonesFrom(context, "Users", 0, 0, int.MaxValue)
                        .ToList();

                    Assert.Equal(1, tombstones.Count);

                    var tombstone = tombstones[0];

                    Assert.True(tombstone.StorageId > 0);
                    Assert.Equal(result.Etag, tombstone.DeletedEtag);
                    Assert.Equal(result.Etag + 1, tombstone.Etag);
                    Assert.Equal(result.Id, tombstone.LowerId);
                }
            }
        }

        [Fact]
        public async Task Cleanup()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = AutoMapIndex.CreateNew(new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                }
            }), database))
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                    {
                        ["Name"] = "John",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Users"
                        }
                    }))
                    {
                        database.DocumentsStorage.Put(context, "key/1", null, doc);
                    }

                    using (var doc = CreateDocument(context, "key/2", new DynamicJsonValue
                    {
                        ["Name"] = "Edward",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Users"
                        }
                    }))
                    {
                        database.DocumentsStorage.Put(context, "key/2", null, doc);
                    }

                    using (var doc = CreateDocument(context, "key/3", new DynamicJsonValue
                    {
                        ["Name"] = "William",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Users"
                        }
                    }))
                    {
                        database.DocumentsStorage.Put(context, "key/3", null, doc);
                    }

                    tx.Commit();
                }

                var batchStats = new IndexingRunStats();
                var stats = new IndexingStatsScope(batchStats);
                index.DoIndexingWork(stats, CancellationToken.None);

                var tombstones = index.GetLastProcessedTombstonesPerCollection();
                Assert.Equal(1, tombstones.Count);
                Assert.Equal(0, tombstones["Users"]);

                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                    Assert.Equal(0, count);
                }

                await database.TombstoneCleaner.ExecuteCleanup();

                using (var tx = context.OpenWriteTransaction())
                {
                    database.DocumentsStorage.Delete(context, "key/1", null);

                    tx.Commit();
                }

                tombstones = index.GetLastProcessedTombstonesPerCollection();
                Assert.Equal(1, tombstones.Count);
                Assert.Equal(0, tombstones["Users"]);

                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                    Assert.Equal(1, count);
                }

                await database.TombstoneCleaner.ExecuteCleanup();

                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                    Assert.Equal(1, count);
                }

                batchStats = new IndexingRunStats();
                stats = new IndexingStatsScope(batchStats);
                index.DoIndexingWork(stats, CancellationToken.None);

                tombstones = index.GetLastProcessedTombstonesPerCollection();
                Assert.Equal(1, tombstones.Count);
                Assert.Equal(4, tombstones["Users"]);

                using (var tx = context.OpenWriteTransaction())
                {
                    database.DocumentsStorage.Delete(context, "key/2", null);

                    tx.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                    Assert.Equal(2, count);
                }

                await database.TombstoneCleaner.ExecuteCleanup();

                using (context.OpenReadTransaction())
                {
                    var list = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal(5, list[0].Etag);
                }
            }
        }
    }
}
