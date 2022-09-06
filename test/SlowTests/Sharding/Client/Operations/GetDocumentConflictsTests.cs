using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations
{
    public class GetDocumentConflictsTests : RavenTestBase
    {
        const string _suffix = "suffix";

        public GetDocumentConflictsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task CanGetReplicationDocumentConflicts()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = await GetDocumentDatabaseInstanceFor(store, store.Database);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);

                using (context.OpenReadTransaction())
                {
                    var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsFrom(context, etag: 0).ToList();
                    var expected = 20; // 2 conflicts per doc
                    Assert.Equal(expected, conflicts.Count);

                    var totalResults = db.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context);
                    expected = 10;
                    Assert.Equal(expected, totalResults);

                    var documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(start: 0, pageSize: 2));
                    Assert.Equal(2, documentsConflict.Results.Count);

                    var nextStart = 0L;
                    foreach (var conflict in documentsConflict.Results)
                        nextStart += conflict.ScannedResults;
                    
                    documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(start: nextStart + 1 /*skip scanned entries and start from next one*/, pageSize: 10));
                    Assert.Equal(8, documentsConflict.Results.Count);

                    documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(start: 0, pageSize: 5));
                    Assert.Equal(5, documentsConflict.Results.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task CanGetReplicationDocumentConflictsForSharding()
        {
            using var store = Sharding.GetDocumentStore();

            int bucket = ShardHelper.GetBucket(_suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            var db = await Sharding.GetShardedDocumentDatabaseForBucketAsync(store.Database, bucket);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);

                using (context.OpenReadTransaction())
                {
                    var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsByBucketFrom(context, bucket, etag: 0).ToList();
                    Assert.Equal(20, conflicts.Count); // 2 conflicts per doc

                    var documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(start: 0, pageSize: 5));
                    Assert.Equal(5, documentsConflict.Results.Count);

                    documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(continuationToken: documentsConflict.ContinuationToken));
                    Assert.Equal(5, documentsConflict.Results.Count);
                    Assert.Equal(10, documentsConflict.TotalResults);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task CanGetReplicationDocumentConflictsForSharding2()
        {
            using var store = Sharding.GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}");
                }

                await session.SaveChangesAsync();
            }

            DateTime now = DateTime.UtcNow;
            var rand = new Random();
            var totalResults = 0;

            var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database);
            foreach (var task in dbs)
            {
                var db = await task;

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var ids = new LazyStringValue[10];
                    int i = 0;
                    using (context.OpenReadTransaction())
                    {
                        var docCount = db.DocumentsStorage.GetNumberOfDocuments(context);
                        if (docCount == 0)
                            continue;

                        foreach (var docId in db.DocumentsStorage.GetAllIds(context))
                        {
                            ids[i++] = docId;
                            if(i > 9)
                                break;
                        }
                    }

                    using (context.OpenWriteTransaction())
                    {
                        i = 0;
                        foreach (var id in ids)
                        {
                            var doc = context.ReadObject(new DynamicJsonValue(), id);
                            db.DocumentsStorage.ConflictsStorage.AddConflict(context, id, now.AddMinutes(rand.Next(100, 10000)).Ticks, doc, $"A:{i}-A1B2C3D4E5F6G7H8I9", "users",
                                DocumentFlags.None);
                            i++;
                        }

                        totalResults += i;
                        context.Transaction.Commit();
                    }
                }
            }
           
            var documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(start: 0, pageSize: 5));
            Assert.Equal(5, documentsConflict.Results.Count);

            documentsConflict = await store.Maintenance.SendAsync(new GetConflictsOperation(continuationToken: documentsConflict.ContinuationToken));
            Assert.Equal(5, documentsConflict.Results.Count);
            Assert.Equal(totalResults, documentsConflict.TotalResults);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task CanGetReplicationDocumentConflictsByDocId()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = await GetDocumentDatabaseInstanceFor(store, store.Database);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);

                using (context.OpenReadTransaction())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}${_suffix}";
                        var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id).ToList();
                        Assert.Equal(2, conflicts.Count); // 2 conflicts per doc

                        var documentConflicts = await store.Maintenance.SendAsync(new GetDocumentConflictsOperation(docId: id));
                        Assert.Equal(2, documentConflicts.Results.Length);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task CanGetReplicationDocumentConflictsByDocIdForSharding()
        {
            using var store = Sharding.GetDocumentStore();

            int bucket = ShardHelper.GetBucket(_suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            var db = await Sharding.GetShardedDocumentDatabaseForBucketAsync(store.Database, bucket);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);
            }

            for (int i = 0; i < 10; i++)
            {
                var id = $"users/{i}${_suffix}";
                var documentConflicts = await store.Maintenance.SendAsync(new GetDocumentConflictsOperation(docId: id));
                Assert.Equal(2, documentConflicts.Results.Length);
            }
        }

        private void AddConflicts(DocumentsOperationContext context, DocumentsStorage storage)
        {
            using (context.OpenWriteTransaction())
            {
                // add some conflicts
                for (int i = 0; i < 10; i++)
                {
                    var id = $"users/{i}${_suffix}";
                    var doc = context.ReadObject(new DynamicJsonValue(), id);
                    storage.ConflictsStorage.AddConflict(context, id, DateTime.UtcNow.Ticks, doc, $"A:{i}-A1B2C3D4E5F6G7H8I9", "users", DocumentFlags.None);
                }

                context.Transaction.Commit();
            }
        }

        private class GetDocumentConflictsOperation : IMaintenanceOperation<GetConflictsResult>
        {
            private readonly string _docId;

            public GetDocumentConflictsOperation(string docId)
            {
                _docId = docId;
            }

            public RavenCommand<GetConflictsResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetConflictsCommand(id: _docId);
            }
        }
    }
}
