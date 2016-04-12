using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Raven.Tests.Core;
using Sparrow.Json;
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
            {
                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                {
                    PutResult result;
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                        {
                            ["Name"] = "John",
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.RavenEntityName] = "Users"
                            }
                        }))
                        {
                            result = database.DocumentsStorage.Put(context, "key/1", null, doc);
                        }

                        tx.Commit();
                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        Assert.True(database.DocumentsStorage.Delete(context, "key/1", null));

                        tx.Commit();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var tombstones = database
                            .DocumentsStorage
                            .GetTombstonesAfter(context, "Users", 0, 0, int.MaxValue)
                            .ToList();

                        Assert.Equal(1, tombstones.Count);

                        var tombstone = tombstones[0];

                        Assert.True(tombstone.StorageId > 0);
                        Assert.Equal(result.ETag, tombstone.DeletedEtag);
                        Assert.Equal(result.ETag + 1, tombstone.Etag);
                        Assert.Equal(result.Key, tombstone.Key);
                    }
                }
            }
        }

        [Fact]
        public void Cleanup()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = AutoMapIndex.CreateNew(1, new AutoMapIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "key/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/2", null, doc);
                            }

                            using (var doc = CreateDocument(context, "key/3", new DynamicJsonValue
                            {
                                ["Name"] = "William",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/3", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingBatchStats();
                        index.DoIndexingWork(batchStats, CancellationToken.None);

                        var tombstones = index.GetLastProcessedDocumentTombstonesPerCollection();
                        Assert.Equal(1, tombstones.Count);
                        Assert.Equal(0, tombstones["Users"]);

                        using (context.OpenReadTransaction())
                        {
                            var count = database.DocumentsStorage.GetTombstonesAfter(context, "Users", 0, 0, 128).Count();
                            Assert.Equal(0, count);
                        }

                        database.DocumentTombstoneCleaner.ExecuteCleanup(null);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "key/1", null);

                            tx.Commit();
                        }

                        tombstones = index.GetLastProcessedDocumentTombstonesPerCollection();
                        Assert.Equal(1, tombstones.Count);
                        Assert.Equal(0, tombstones["Users"]);

                        using (context.OpenReadTransaction())
                        {
                            var count = database.DocumentsStorage.GetTombstonesAfter(context, "Users", 0, 0, 128).Count();
                            Assert.Equal(1, count);
                        }

                        database.DocumentTombstoneCleaner.ExecuteCleanup(null);

                        using (context.OpenReadTransaction())
                        {
                            var count = database.DocumentsStorage.GetTombstonesAfter(context, "Users", 0, 0, 128).Count();
                            Assert.Equal(1, count);
                        }

                        batchStats = new IndexingBatchStats();
                        index.DoIndexingWork(batchStats, CancellationToken.None);

                        tombstones = index.GetLastProcessedDocumentTombstonesPerCollection();
                        Assert.Equal(1, tombstones.Count);
                        Assert.Equal(4, tombstones["Users"]);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "key/2", null);

                            tx.Commit();
                        }

                        using (context.OpenReadTransaction())
                        {
                            var count = database.DocumentsStorage.GetTombstonesAfter(context, "Users", 0, 0, 128).Count();
                            Assert.Equal(2, count);
                        }

                        database.DocumentTombstoneCleaner.ExecuteCleanup(null);

                        using (context.OpenReadTransaction())
                        {
                            var list = database.DocumentsStorage.GetTombstonesAfter(context, "Users", 0, 0, 128).ToList();
                            Assert.Equal(1, list.Count);
                            Assert.Equal(5, list[0].Etag);
                        }
                    }
                }
            }
        }

        private static BlittableJsonReaderObject CreateDocument(JsonOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }
    }
}