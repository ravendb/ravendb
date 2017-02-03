using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Queries;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Xunit;
using Constants = Raven.Abstractions.Data.Constants;

namespace FastTests.Server.Documents.Indexing.Auto
{
    public class BasicAutoMapIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task CheckDispose()
        {
            using (var database = CreateDocumentDatabase())
            {
                var index = AutoMapIndex.CreateNew(1, new AutoMapIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database);
                index.Dispose();

                index.Dispose();// can dispose twice

                Assert.Throws<ObjectDisposedException>(() => index.Start());

                var ex = await Record.ExceptionAsync(() => index.Query(new IndexQueryServerSide(), null, OperationCancelToken.None));
                Assert.IsType<ObjectDisposedException>(ex);

                index = AutoMapIndex.CreateNew(1, new AutoMapIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database);
                index.Start();
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoMapIndex.CreateNew(1, new AutoMapIndexDefinition("Users", new[] { new IndexField
                    {
                        Name = "Name",
                        Highlighted = false,
                        Storage = FieldStorage.No
                    } }), database);
                    index.Start();

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        [Fact]
        public void CanDispose()
        {
            using (var database = CreateDocumentDatabase(runInMemory: false))
            {
                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name1",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } })));
                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name2",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } })));
            }
        }

        [Fact]
        public void CanPersist()
        {
            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var name1 = new IndexField
                {
                    Name = "Name1",
                    Highlighted = true,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.String
                };
                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 })));
                var name2 = new IndexField
                {
                    Name = "Name2",
                    Highlighted = false,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.NumericDefault
                };
                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name2 })));

                var index2 = database.IndexStore.GetIndex(2);
                index2.SetLock(IndexLockMode.LockedError);
                index2.SetPriority(IndexPriority.Low);
                index2.SetState(IndexState.Disabled);
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x => x.IndexId)
                    .ToList();

                Assert.Equal(2, indexes.Count);

                Assert.Equal(1, indexes[0].IndexId);
                Assert.Equal(1, indexes[0].Definition.Collections.Length);
                Assert.Equal("Users", indexes[0].Definition.Collections[0]);
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Equal("Name1", indexes[0].Definition.MapFields["Name1"].Name);
                Assert.Equal(SortOptions.String, indexes[0].Definition.MapFields["Name1"].SortOption);
                Assert.True(indexes[0].Definition.MapFields["Name1"].Highlighted);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Priority);
                Assert.Equal(IndexState.Normal, indexes[0].State);

                Assert.Equal(2, indexes[1].IndexId);
                Assert.Equal(1, indexes[1].Definition.Collections.Length);
                Assert.Equal("Users", indexes[1].Definition.Collections[0]);
                Assert.Equal(1, indexes[1].Definition.MapFields.Count);
                Assert.Equal("Name2", indexes[1].Definition.MapFields["Name2"].Name);
                Assert.Equal(SortOptions.NumericDefault, indexes[1].Definition.MapFields["Name2"].SortOption);
                Assert.False(indexes[1].Definition.MapFields["Name2"].Highlighted);
                Assert.Equal(IndexLockMode.LockedError, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.Low, indexes[1].Priority);
                Assert.Equal(IndexState.Disabled, indexes[1].State);
            }
        }

        [Fact]
        public void CanDelete()
        {
            using (var database = CreateDocumentDatabase())
                CanDelete(database);

            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
                CanDelete(database);
        }

        private static void CanDelete(DocumentDatabase database)
        {
            var index1 =
                database.IndexStore.CreateIndex(
                    new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
            var path1 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                database.IndexStore.GetIndex(index1).GetIndexNameSafeForFileSystem());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var index2 =
                database.IndexStore.CreateIndex(
                    new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name2" } }));
            var path2 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                database.IndexStore.GetIndex(index2).GetIndexNameSafeForFileSystem());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            Assert.Equal(2, database.IndexStore.GetIndexesForCollection("Users").Count());

            database.IndexStore.DeleteIndex(index1);

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path1) == false, TimeSpan.FromSeconds(5)));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(1, indexes.Count);
            Assert.Equal(index2, indexes[0].IndexId);

            database.IndexStore.DeleteIndex(index2);

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(0, indexes.Count);
        }

        [Fact]
        public void CanReset()
        {
            using (var database = CreateDocumentDatabase())
                CanReset(database);

            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
                CanReset(database);
        }

        private static void CanReset(DocumentDatabase database)
        {
            var index1 =
                database.IndexStore.CreateIndex(
                    new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));



            var path1 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                database.IndexStore.GetIndex(index1).GetIndexNameSafeForFileSystem());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var index2 =
                database.IndexStore.CreateIndex(
                    new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name2" } }));
            var path2 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                database.IndexStore.GetIndex(index2).GetIndexNameSafeForFileSystem());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            Assert.Equal(2, database.IndexStore.GetIndexesForCollection("Users").Count());

            var index3 = database.IndexStore.ResetIndex(index1);
            var path3 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                database.IndexStore.GetIndex(index3).GetIndexNameSafeForFileSystem());

            Assert.NotEqual(index3, index1);
            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path3));

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path1) == false, TimeSpan.FromSeconds(5)));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);

            var index4 = database.IndexStore.ResetIndex(index2);
            var path4 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                database.IndexStore.GetIndex(index4).GetIndexNameSafeForFileSystem());

            Assert.NotEqual(index4, index2);
            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path4));

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);
        }

        [Fact]
        public void SimpleIndexing()
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
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
                                [Constants.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "key/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/2", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        while (index.DoIndexingWork(scope, CancellationToken.None))
                        {

                        }
                        Assert.Equal(2, index.GetLastMappedEtagsForDebug().Values.Min());
                        Assert.Equal(2, batchStats.MapAttempts);
                        Assert.Equal(2, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        var now = SystemTime.UtcNow;
                        index._indexStorage.UpdateStats(now, batchStats);

                        var stats = index.GetStats();
                        Assert.Equal(index.IndexId, stats.Id);
                        Assert.Equal(index.Name, stats.Name);
                        Assert.False(stats.IsInvalidIndex);
                        Assert.False(stats.IsTestIndex);
                        Assert.Equal(IndexType.AutoMap, stats.Type);
                        Assert.Equal(2, stats.EntriesCount);
                        Assert.Equal(2, stats.MapAttempts);
                        Assert.Equal(0, stats.MapErrors);
                        Assert.Equal(2, stats.MapSuccesses);
                        Assert.Equal(1, stats.Collections.Count);
                        Assert.Equal(2, stats.Collections.First().Value.LastProcessedDocumentEtag);
                        Assert.Equal(now, stats.LastIndexingTime);
                        Assert.NotNull(stats.LastQueryingTime);
                        Assert.Equal(IndexLockMode.Unlock, stats.LockMode);
                        Assert.Equal(IndexPriority.Normal, stats.Priority);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "key/3", new DynamicJsonValue
                            {
                                ["Name"] = "William",
                                [Constants.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/3", null, doc);
                            }

                            tx.Commit();
                        }

                        batchStats = new IndexingRunStats();
                        scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);
                        Assert.Equal(3, index.GetLastMappedEtagsForDebug().Values.Min());
                        Assert.Equal(1, batchStats.MapAttempts);
                        Assert.Equal(1, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        now = SystemTime.UtcNow;
                        index._indexStorage.UpdateStats(now, batchStats);

                        stats = index.GetStats();
                        Assert.Equal(index.IndexId, stats.Id);
                        Assert.Equal(index.Name, stats.Name);
                        Assert.False(stats.IsInvalidIndex);
                        Assert.False(stats.IsTestIndex);
                        Assert.Equal(IndexType.AutoMap, stats.Type);
                        Assert.Equal(3, stats.EntriesCount);
                        Assert.Equal(3, stats.MapAttempts);
                        Assert.Equal(0, stats.MapErrors);
                        Assert.Equal(3, stats.MapSuccesses);
                        Assert.Equal(1, stats.Collections.Count);
                        Assert.Equal(3, stats.Collections.First().Value.LastProcessedDocumentEtag);
                        Assert.Equal(now, stats.LastIndexingTime);
                        Assert.NotNull(stats.LastQueryingTime);
                        Assert.Equal(IndexLockMode.Unlock, stats.LockMode);
                        Assert.Equal(IndexPriority.Normal, stats.Priority);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "key/1", null);

                            tx.Commit();
                        }

                        batchStats = new IndexingRunStats();
                        scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);
                        Assert.Equal(4, index.GetLastProcessedDocumentTombstonesPerCollection().Values.Min());
                        Assert.Equal(0, batchStats.MapAttempts);
                        Assert.Equal(0, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        now = SystemTime.UtcNow;
                        index._indexStorage.UpdateStats(now, batchStats);

                        stats = index.GetStats();
                        Assert.Equal(index.IndexId, stats.Id);
                        Assert.Equal(index.Name, stats.Name);
                        Assert.False(stats.IsInvalidIndex);
                        Assert.False(stats.IsTestIndex);
                        Assert.Equal(IndexType.AutoMap, stats.Type);
                        Assert.Equal(2, stats.EntriesCount);
                        Assert.Equal(3, stats.MapAttempts);
                        Assert.Equal(0, stats.MapErrors);
                        Assert.Equal(3, stats.MapSuccesses);
                        Assert.Equal(1, stats.Collections.Count);
                        Assert.Equal(3, stats.Collections.First().Value.LastProcessedDocumentEtag);
                        Assert.Equal(now, stats.LastIndexingTime);
                        Assert.NotNull(stats.LastQueryingTime);
                        Assert.Equal(IndexLockMode.Unlock, stats.LockMode);
                        Assert.Equal(IndexPriority.Normal, stats.Priority);
                    }
                }
            }
        }

        [Fact]
        public void WriteErrors()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = AutoMapIndex.CreateNew(
                    1,
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } }),
                    database))
                {
                    index.Start();
                    Assert.Equal(IndexRunningStatus.Running, index.Status);

                    IndexStats stats;
                    var batchStats = new IndexingRunStats();
                    var scope = new IndexingStatsScope(batchStats);
                    var iwe = new IndexWriteException();
                    for (int i = 0; i < 10; i++)
                    {
                        stats = index.GetStats();
                        Assert.Equal(IndexPriority.Normal, stats.Priority);
                        index.HandleWriteErrors(scope, iwe);
                    }

                    stats = index.GetStats();
                    Assert.Equal(IndexState.Error, stats.State);
                    Assert.True(SpinWait.SpinUntil(() => index.Status == IndexRunningStatus.Paused, TimeSpan.FromSeconds(5)));
                }
            }
        }

        [Fact]
        public void Errors2()
        {
            var times = new[]
            {
                "2016-04-15T15:48:27.4372303Z",
                "2016-04-15T15:48:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4372303Z",
                "2016-04-15T15:49:27.4528737Z",
                "2016-04-15T15:49:27.4528737Z",
                "2016-04-15T15:49:27.4528737Z",
                "2016-04-15T15:49:27.4528737Z",
                "2016-04-15T15:49:27.4528737Z",
                "2016-04-15T15:49:27.4533779Z",
                "2016-04-15T15:49:27.4533779Z",
                "2016-04-15T15:49:27.4533779Z",
                "2016-04-15T15:49:27.4533779Z",
                "2016-04-15T15:49:27.4533779Z",
                "2016-04-15T15:49:27.4533779Z",
                "2016-04-15T15:49:27.4538960Z",
                "2016-04-15T15:49:27.4538960Z",
                "2016-04-15T15:49:27.4538960Z",
                "2016-04-15T15:49:27.4538960Z",
                "2016-04-15T15:49:27.4538960Z",
                "2016-04-15T15:49:27.4538960Z",
                "2016-04-15T15:49:27.4543969Z",
                "2016-04-15T15:49:27.4543969Z",
                "2016-04-15T15:49:27.4543969Z",
                "2016-04-15T15:49:27.4543969Z",
                "2016-04-15T15:49:27.4543969Z",
                "2016-04-15T15:49:27.4543969Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4548965Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4553966Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4558974Z",
                "2016-04-15T15:49:27.4563979Z",
                "2016-04-15T15:49:27.4563979Z",
                "2016-04-15T15:49:27.4563979Z",
                "2016-04-15T15:49:27.4563979Z",
                "2016-04-15T15:49:27.4563979Z",
                "2016-04-15T15:49:27.4563979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4568979Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4573984Z",
                "2016-04-15T15:49:27.4578988Z",
                "2016-04-15T15:49:27.4578988Z",
                "2016-04-15T15:49:27.4578988Z",
                "2016-04-15T15:49:27.4578988Z",
                "2016-04-15T15:49:27.4578988Z",
                "2016-04-15T15:49:27.4583993Z",
                "2016-04-15T15:49:27.4583993Z",
                "2016-04-15T15:49:27.4583993Z",
                "2016-04-15T15:49:27.4588993Z",
                "2016-04-15T15:49:27.4588993Z",
                "2016-04-15T15:49:27.4588993Z",
                "2016-04-15T15:49:27.4588993Z",
                "2016-04-15T15:49:27.4588993Z",
                "2016-04-15T15:49:27.4588993Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4594010Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
                "2016-04-15T15:49:27.4750453Z",
            };
            using (var database = CreateDocumentDatabase())
            {
                using (var index = AutoMapIndex.CreateNew(
                    1,
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } }),
                    database))
                {
                    var stats = new IndexingRunStats();

                    stats.AddWriteError(new IndexWriteException());
                    stats.AddAnalyzerError(new IndexAnalyzerException());

                    for (int i = 0; i < times.Length; i++)
                    {
                        var now = DateTime.Parse(times[i]);
                        stats.Errors[0].Timestamp = now;
                        stats.Errors[1].Timestamp = now;
                        index._indexStorage.UpdateStats(now, stats);
                    }
                }
            }
        }

        [
            Fact]
        public void Errors()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = AutoMapIndex.CreateNew(
                    1,
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } }),
                    database))
                {
                    var stats = new IndexingRunStats();
                    index._indexStorage.UpdateStats(SystemTime.UtcNow, stats);

                    Assert.Equal(0, index.GetErrors().Count);

                    stats.AddWriteError(new IndexWriteException());
                    stats.AddAnalyzerError(new IndexAnalyzerException());

                    index._indexStorage.UpdateStats(SystemTime.UtcNow, stats);

                    var errors = index.GetErrors();
                    Assert.Equal(2, errors.Count);
                    Assert.Equal("Write", errors[0].Action);
                    Assert.Equal("Analyzer", errors[1].Action);

                    for (int i = 0; i < IndexStorage.MaxNumberOfKeptErrors; i++)
                    {
                        var now = SystemTime.UtcNow.AddMinutes(1);
                        stats.Errors[0].Timestamp = now;
                        stats.Errors[1].Timestamp = now;
                        index._indexStorage.UpdateStats(now, stats);
                    }

                    errors = index.GetErrors();
                    Assert.Equal(IndexStorage.MaxNumberOfKeptErrors, errors.Count);
                }
            }
        }

        [Fact]
        public async Task AutoIndexesShouldBeMarkedAsIdleAndDeleted()
        {
            using (var database = CreateDocumentDatabase())
            {
                var index0Id = database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Job", Highlighted = false, Storage = FieldStorage.No } }));
                var index0 = database.IndexStore.GetIndex(index0Id);

                index0.SetState(IndexState.Idle);

                database.IndexStore.RunIdleOperations(); // young idle index should be removed

                index0 = database.IndexStore.GetIndex(index0Id);
                Assert.Null(index0);

                var index1Id = database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } }));
                var index2Id = database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Age", Highlighted = false, Storage = FieldStorage.No } }));

                var index1 = database.IndexStore.GetIndex(index1Id);
                var index2 = database.IndexStore.GetIndex(index2Id);
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    await index1.Query(new IndexQueryServerSide(), context, OperationCancelToken.None); // last querying time
                }
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    await index2.Query(new IndexQueryServerSide(), context, OperationCancelToken.None); // last querying time
                }

                Assert.Equal(IndexPriority.Normal, index1.Priority);
                Assert.Equal(IndexPriority.Normal, index2.Priority);

                database.IndexStore.RunIdleOperations(); // nothing should happen because difference between querying time between those two indexes is less than TimeToWaitBeforeMarkingAutoIndexAsIdle

                index1 = database.IndexStore.GetIndex(index1Id);
                index2 = database.IndexStore.GetIndex(index2Id);

                Assert.Equal(IndexPriority.Normal, index1.Priority);
                Assert.Equal(IndexPriority.Normal, index2.Priority);

                database.Time.UtcDateTime = () => DateTime.UtcNow.Add(database.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    await index1.Query(new IndexQueryServerSide(), context, OperationCancelToken.None); // last querying time
                }

                database.IndexStore.RunIdleOperations(); // this will mark index2 as idle, because the difference between two indexes and index last querying time is more than TimeToWaitBeforeMarkingAutoIndexAsIdle

                index1 = database.IndexStore.GetIndex(index1Id);
                index2 = database.IndexStore.GetIndex(index2Id);

                Assert.Equal(IndexPriority.Normal, index1.Priority);
                Assert.Equal(IndexState.Idle, index2.State);

                var now = database.Time.GetUtcNow();
                database.Time.UtcDateTime = () =>
                        now.Add(TimeSpan.FromSeconds(1))
                           .Add(database.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan);

                database.IndexStore.RunIdleOperations(); // nothing should happen here, because age will be greater than 2x TimeToWaitBeforeMarkingAutoIndexAsIdle but less than TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle

                index1 = database.IndexStore.GetIndex(index1Id);
                index2 = database.IndexStore.GetIndex(index2Id);

                Assert.Equal(IndexPriority.Normal, index1.Priority);
                Assert.Equal(IndexState.Idle, index2.State);

                now = database.Time.GetUtcNow();
                database.Time.UtcDateTime = () => now.Add(database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan);

                database.IndexStore.RunIdleOperations(); // this will delete index2

                index1 = database.IndexStore.GetIndex(index1Id);
                index2 = database.IndexStore.GetIndex(index2Id);

                Assert.Equal(IndexPriority.Normal, index1.Priority);
                Assert.Null(index2);
            }
        }

        [Fact]
        public void IndexCreationOptions()
        {
            using (var database = CreateDocumentDatabase())
            {
                var definition1 = new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } });
                var definition2 = new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } });

                var indexId1 = database.IndexStore.CreateIndex(definition1);
                var indexId2 = database.IndexStore.CreateIndex(definition2);

                Assert.Equal(indexId1, indexId2);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());

                var definition3 = new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.Yes } });

                var indexId3 = database.IndexStore.CreateIndex(definition3);

                Assert.NotEqual(indexId1, indexId3);
                Assert.Null(database.IndexStore.GetIndex(indexId1));
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());
            }
        }

        [Fact]
        public void LockMode()
        {
            using (var database = CreateDocumentDatabase())
            {
                var definition1 = new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } });
                var definition2 = new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name", Highlighted = false, Storage = FieldStorage.No } });

                var indexId1 = database.IndexStore.CreateIndex(definition1);
                var index1 = database.IndexStore.GetIndex(indexId1);
                index1.SetLock(IndexLockMode.LockedIgnore);

                var indexId2 = database.IndexStore.CreateIndex(definition2);
                Assert.Equal(indexId1, indexId2);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());

                index1.SetLock(IndexLockMode.LockedError);
                Assert.Throws<InvalidOperationException>(() => database.IndexStore.CreateIndex(definition2));
            }
        }

        [Fact]
        public void IndexLoadErrorCreatesFaultyInMemoryIndexFakeAndAddsAlert()
        {
            var path = NewDataPath();
            string indexStoragePath;
            string indexName;

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var name1 = new IndexField
                {
                    Name = "Name1",
                    Highlighted = true,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.String
                };

                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 })));
                var index = database.IndexStore.GetIndex(1);
                indexName = index.Name;

                indexStoragePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                    index.GetIndexNameSafeForFileSystem());
            }

            IOExtensions.DeleteFile(Path.Combine(indexStoragePath, "headers.one"));
            IOExtensions.DeleteFile(Path.Combine(indexStoragePath, "headers.two"));

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration => configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = false))
            {
                var index = database
                    .IndexStore
                    .GetIndex(1);

                Assert.IsType<FaultyInMemoryIndex>(index);
                Assert.Equal(IndexState.Error, index.State);
                Assert.Equal(indexName, index.Name);

                // TODO arek: verify that alert was created as well
            }
        }

        [Fact]
        public void CanDeleteFaultyIndex()
        {
            var path = NewDataPath();
            string indexStoragePath;
            string indexSafeName;

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var name1 = new IndexField
                {
                    Name = "Name1",
                    Highlighted = true,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.String
                };

                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 })));
                var index = database.IndexStore.GetIndex(1);
                indexSafeName = index.GetIndexNameSafeForFileSystem();

                indexStoragePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                    index.GetIndexNameSafeForFileSystem());
            }

            Assert.True(Directory.Exists(indexStoragePath));
            IOExtensions.DeleteDirectory(indexStoragePath);
            Directory.CreateDirectory(indexStoragePath); // worst case, we have no info

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration =>
            {
                configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = false;
            }))
            {
                var index = database
                    .IndexStore
                    .GetIndex(1);

                Assert.IsType<FaultyInMemoryIndex>(index);
                Assert.Equal(IndexState.Error, index.State);
                Assert.Equal(indexSafeName, index.GetIndexNameSafeForFileSystem());

                database.IndexStore.DeleteIndex(index.IndexId);

                for (int i = 0; i < 5; i++)
                {
                    if (Directory.Exists(indexStoragePath) == false)
                        return;
                    Thread.Sleep(16);
                }

                Assert.False(true, indexStoragePath + " exists");
            }
        }

        [Fact]
        public void CanDeleteFaultyIndexWithNonDefaultPath()
        {
            var path = NewDataPath();
            var additionalPath = NewDataPath();
            string indexStoragePath;
            string indexSafeName;

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration =>
            {
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths), additionalPath);
            }))
            {
                Assert.Equal(1, database.IndexStore.CreateIndex(
                    new IndexDefinition
                    {
                        Name = "Index1",
                        Maps = { "from doc in docs select new { doc.Name }" },
                        Configuration =
                        {
                            {RavenConfiguration.GetKey(x => x.Indexing.StoragePath), additionalPath}
                        }
                    }));
                var index = database.IndexStore.GetIndex(1);
                indexSafeName = index.GetIndexNameSafeForFileSystem();

                indexStoragePath = Path.Combine(index.Configuration.StoragePath.FullPath, index.GetIndexNameSafeForFileSystem());
            }

            Assert.True(Directory.Exists(indexStoragePath));
            IOExtensions.DeleteDirectory(indexStoragePath);
            Directory.CreateDirectory(indexStoragePath); // worst case, we have no info

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration =>
            {
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths), additionalPath);
                configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = false;
            }))
            {
                var index = database
                    .IndexStore
                    .GetIndex(1);

                Assert.IsType<FaultyInMemoryIndex>(index);
                Assert.Equal(IndexState.Error, index.State);
                Assert.Equal(indexSafeName, index.GetIndexNameSafeForFileSystem());

                database.IndexStore.DeleteIndex(index.IndexId);

                for (int i = 0; i < 5; i++)
                {
                    if (Directory.Exists(indexStoragePath) == false)
                        return;
                    Thread.Sleep(16);
                }

                Assert.False(true, indexStoragePath + " exists");
            }
        }
    }
}