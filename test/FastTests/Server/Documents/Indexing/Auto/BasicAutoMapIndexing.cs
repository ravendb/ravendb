using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Queries;
using Raven.Server.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Auto
{
    public class BasicAutoMapIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task CheckDispose()
        {
            using (var database = CreateDocumentDatabase())
            {
                var index = AutoMapIndex.CreateNew(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                } }), database);
                index.Dispose();

                index.Dispose();// can dispose twice

                Assert.Throws<ObjectDisposedException>(() => index.Start());

                var ex = await Record.ExceptionAsync(() => index.Query(new IndexQueryServerSide($"FROM INDEX'{index.Name}'"), null, OperationCancelToken.None));
                Assert.IsType<ObjectDisposedException>(ex);

                index = AutoMapIndex.CreateNew(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                } }), database);
                index.Start();
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoMapIndex.CreateNew(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField
                    {
                        Name = "Name",
                        Storage = FieldStorage.No
                    } }), database);
                    index.Start();

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        [Fact]
        public async Task CanPersist()
        {
            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var dbName = database.Name;

                var name1 = new AutoIndexField
                {
                    Name = "Name1",
                    Storage = FieldStorage.No,
                };
                Assert.NotNull(await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 })));
                var name2 = new AutoIndexField
                {
                    Name = "Name2",
                    Storage = FieldStorage.No,
                    Indexing = AutoFieldIndexing.Default | AutoFieldIndexing.Search
                };

                var index2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name2 }));
                Assert.NotNull(index2);

                var task1 = database.IndexStore.SetLock(index2.Name, IndexLockMode.LockedError);
                var task2 = database.IndexStore.SetPriority(index2.Name, IndexPriority.Low);
                index2.SetState(IndexState.Disabled);
                var task = Task.WhenAll(task1, task2);
               
                await Assert.ThrowsAsync<NotSupportedException>(async () => await task);
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", task1.Exception.InnerException.Message);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                database = await GetDatabase(dbName);

                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x=>x.Name.Length)
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Equal(1, indexes[0].Definition.Collections.Count);
                Assert.Equal("Users", indexes[0].Definition.Collections.Single());
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Equal("Name1", indexes[0].Definition.MapFields["Name1"].Name);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Definition.Priority);
                Assert.Equal(IndexState.Normal, indexes[0].State);

                Assert.Equal(1, indexes[1].Definition.Collections.Count);
                Assert.Equal("Users", indexes[1].Definition.Collections.Single());
                Assert.Equal(1, indexes[1].Definition.MapFields.Count);
                Assert.Equal("Name2", indexes[1].Definition.MapFields["Name2"].Name);
                Assert.Equal(AutoFieldIndexing.Search | AutoFieldIndexing.Default, indexes[1].Definition.MapFields["Name2"].As<AutoIndexField>().Indexing);
                Assert.Equal(IndexLockMode.Unlock, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.Low, indexes[1].Definition.Priority);
                Assert.Equal(IndexState.Disabled, indexes[1].State);
            }
        }

        [Fact]
        public async Task CanDelete()
        {
            using (var database = CreateDocumentDatabase())
                await CanDeleteInternal(database);

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
                await CanDeleteInternal(database);
        }

        private static async Task CanDeleteInternal(DocumentDatabase database)
        {
            var def1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name1" } });
            var index1 =
                await database.IndexStore.CreateIndex(
                    def1);
            var path1 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBase.GetIndexNameSafeForFileSystem(def1.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var def2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name2" } });
            var index2 =
                await database.IndexStore.CreateIndex(
                    def2);
            var path2 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBase.GetIndexNameSafeForFileSystem(def2.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            Assert.Equal(2, database.IndexStore.GetIndexesForCollection("Users").Count());

            await database.IndexStore.DeleteIndex(index1.Name);

            if (index1.Configuration.RunInMemory == false)
                Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path1) == false, TimeSpan.FromSeconds(5)));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(1, indexes.Count);

            await database.IndexStore.DeleteIndex(index2.Name);

            if (index1.Configuration.RunInMemory == false)
                Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(0, indexes.Count);
        }

        [Fact]
        public async Task CanReset()
        {
            using (var database = CreateDocumentDatabase())
                await CanResetInternal(database);

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
                await CanResetInternal(database);
        }

        private static async Task CanResetInternal(DocumentDatabase database)
        {
            var def1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name1" } });
            var index1 = await database.IndexStore.CreateIndex(def1);

            var path1 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBase.GetIndexNameSafeForFileSystem(def1.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var def2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name2" } });
            var index2 = await database.IndexStore.CreateIndex(def2);
            var path2 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBase.GetIndexNameSafeForFileSystem(def2.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            var indexesAfterReset = database.IndexStore.GetIndexesForCollection("Users");
            Assert.Equal(2, indexesAfterReset.Count());

            var index3 = database.IndexStore.ResetIndex(index1.Name);
            var path3 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath, IndexDefinitionBase.GetIndexNameSafeForFileSystem(def1.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path3));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);

            var index4 = database.IndexStore.ResetIndex(index2.Name);
            var path4 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath, IndexDefinitionBase.GetIndexNameSafeForFileSystem(def2.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path4));

            //Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);
        }

        [Fact]
        public void SimpleIndexing()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = AutoMapIndex.CreateNew(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField
                {
                    Name = "Name",
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
                        Assert.Equal(index.Name, stats.Name);
                        Assert.False(stats.IsInvalidIndex);
#if FEATURE_TEST_INDEX
                        Assert.False(stats.IsTestIndex);
#endif
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
   
                        Assert.Equal(index.Name, stats.Name);
                        Assert.False(stats.IsInvalidIndex);
#if FEATURE_TEST_INDEX
                        Assert.False(stats.IsTestIndex);
#endif
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
                        Assert.Equal(4, index.GetLastProcessedTombstonesPerCollection().Values.Min());
                        Assert.Equal(0, batchStats.MapAttempts);
                        Assert.Equal(0, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        now = SystemTime.UtcNow;
                        index._indexStorage.UpdateStats(now, batchStats);

                        stats = index.GetStats();
                        Assert.Equal(index.Name, stats.Name);
                        Assert.False(stats.IsInvalidIndex);
#if FEATURE_TEST_INDEX
                        Assert.False(stats.IsTestIndex);
#endif
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
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } }),
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
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } }),
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
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } }),
                    database))
                {
                    var stats = new IndexingRunStats();
                    index._indexStorage.UpdateStats(SystemTime.UtcNow, stats);

                    Assert.Equal(0, index.GetErrors().Count);
                    Assert.Equal(0, index.GetErrorCount());

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
                    Assert.Equal(index.GetErrorCount(), errors.Count);
                }
            }
        }

        [Fact]
        public async Task AutoIndexesShouldBeMarkedAsIdleAndDeleted()
        {
            using (var database = CreateDocumentDatabase())
            {
                var index0 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Job", Storage = FieldStorage.No } }));
                index0.SetState(IndexState.Idle);

                database.IndexStore.RunIdleOperations(); // young idle index should be removed

                index0 = database.IndexStore.GetIndex(index0.Name);
                Assert.Null(index0);

                var index1 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } }));
                var index2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Age", Storage = FieldStorage.No } }));

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    await index1.Query(new IndexQueryServerSide("FROM Users"), context, OperationCancelToken.None); // last querying time
                }
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    await index2.Query(new IndexQueryServerSide("FROM Users"), context, OperationCancelToken.None); // last querying time
                }

                Assert.Equal(IndexPriority.Normal, index1.Definition.Priority);
                Assert.Equal(IndexPriority.Normal, index2.Definition.Priority);

                database.IndexStore.RunIdleOperations(); // nothing should happen because difference between querying time between those two indexes is less than TimeToWaitBeforeMarkingAutoIndexAsIdle

                index1 = database.IndexStore.GetIndex(index1.Name);
                index2 = database.IndexStore.GetIndex(index2.Name);

                Assert.Equal(IndexPriority.Normal, index1.Definition.Priority);
                Assert.Equal(IndexPriority.Normal, index2.Definition.Priority);

                database.Time.UtcDateTime = () => DateTime.UtcNow.Add(database.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    await index1.Query(new IndexQueryServerSide("FROM Users"), context, OperationCancelToken.None); // last querying time
                }

                database.IndexStore.RunIdleOperations(); // this will mark index2 as idle, because the difference between two indexes and index last querying time is more than TimeToWaitBeforeMarkingAutoIndexAsIdle

                index1 = database.IndexStore.GetIndex(index1.Name);
                index2 = database.IndexStore.GetIndex(index2.Name);

                Assert.Equal(IndexPriority.Normal, index1.Definition.Priority);
                Assert.Equal(IndexState.Idle, index2.State);

                var now = database.Time.GetUtcNow();
                database.Time.UtcDateTime = () =>
                        now.Add(TimeSpan.FromSeconds(1))
                           .Add(database.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan);

                database.IndexStore.RunIdleOperations(); // should not remove anything, age will be greater than 2x TimeToWaitBeforeMarkingAutoIndexAsIdle but less than TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle

                index1 = database.IndexStore.GetIndex(index1.Name);
                index2 = database.IndexStore.GetIndex(index2.Name);

                Assert.Equal(IndexState.Idle, index1.State);
                Assert.Equal(IndexState.Idle, index2.State);

                now = database.Time.GetUtcNow();
                database.Time.UtcDateTime = () => now.Add(database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan);

                database.IndexStore.RunIdleOperations(); // this will delete indexes

                index1 = database.IndexStore.GetIndex(index1.Name);
                index2 = database.IndexStore.GetIndex(index2.Name);

                Assert.Null(index1);
                Assert.Null(index2);
            }
        }

        [Fact]
        public async Task IndexCreationOptions()
        {
            using (var database = CreateDocumentDatabase())
            {
                var definition1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });
                var definition2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });

                var index1 = await database.IndexStore.CreateIndex(definition1);
                var index2 = await database.IndexStore.CreateIndex(definition2);

                Assert.Equal(index1, index2);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());

                var definition3 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.Yes } });

                var e = (await Assert.ThrowsAsync<RachisApplyException>(() => database.IndexStore.CreateIndex(definition3))).InnerException;

                Assert.Contains("Can not update auto-index", e.Message);
                Assert.NotNull(index1);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());
            }
        }

        [Fact]
        public async Task LockMode()
        {
            using (var database = CreateDocumentDatabase())
            {
                var definition1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });
                var definition2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });

                var index1 = await database.IndexStore.CreateIndex(definition1);
                
                var exception =  Assert.Throws<NotSupportedException>(() => index1.SetLock(IndexLockMode.LockedIgnore));
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", exception.Message);
                
                await database.IndexStore.CreateIndex(definition2);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());

                exception =  Assert.Throws<NotSupportedException>(() => index1.SetLock(IndexLockMode.LockedError));
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", exception.Message);
                
                var index2 = await database.IndexStore.CreateIndex(definition2);
                
                Assert.NotNull(index1);
                Assert.NotNull(index2);
            }
        }

        [Fact]
        public async Task IndexLoadErrorCreatesFaultyInMemoryIndexFakeAndAddsAlert()
        {
            string indexStoragePath;
            string indexName;
            string dbName;

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                dbName = database.Name;

                var name1 = new AutoIndexField
                {
                    Name = "Name1",
                    Storage = FieldStorage.No,
                };

                var index = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 }));
                Assert.NotNull(index);
                indexName = index.Name;

                indexStoragePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                    IndexDefinitionBase.GetIndexNameSafeForFileSystem(indexName));

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                IOExtensions.DeleteDirectory(Path.Combine(indexStoragePath, "Journals"));

                await ModifyDatabaseSettings(dbName, record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                });

                database = await GetDatabase(dbName);

                index = database
                    .IndexStore
                    .GetIndex(indexName);

                Assert.IsType<FaultyInMemoryIndex>(index);
                Assert.Equal(IndexState.Error, index.State);
                Assert.Equal(indexName, index.Name);

                using (database.NotificationCenter.GetStored(out var items))
                {
                    var alerts = items.ToList();
                    Assert.Equal(1, alerts.Count);

                    var readAlert = alerts[0].Json;
                    
                    Assert.Equal(AlertType.IndexStore_IndexCouldNotBeOpened.ToString(), readAlert[nameof(AlertRaised.AlertType)].ToString());
                    Assert.Contains(indexName, readAlert[nameof(AlertRaised.Message)].ToString());
                }
            }
        }

        [Fact]
        public async Task CanDeleteFaultyIndex()
        {
            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var dbName = database.Name;

                var name1 = new AutoIndexField
                {
                    Name = "Name1",
                    Storage = FieldStorage.No,
                };

                var index = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 }));
                Assert.NotNull(index);
                var indexSafeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name);

                var indexStoragePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                    IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name));


                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                Assert.True(Directory.Exists(indexStoragePath));
                
                IOExtensions.DeleteDirectory(Path.Combine(indexStoragePath, "Journals"));

                await ModifyDatabaseSettings(dbName, record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                });

                database = await GetDatabase(dbName);

                index = database
                    .IndexStore
                    .GetIndex(index.Name);

                Assert.IsType<FaultyInMemoryIndex>(index);
                Assert.Equal(IndexState.Error, index.State);
                Assert.Equal(indexSafeName, IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Name));

                await database.IndexStore.DeleteIndex(index.Name);

                for (int i = 0; i < 5; i++)
                {
                    if (Directory.Exists(indexStoragePath) == false)
                        return;
                    Thread.Sleep(16);
                }

                Assert.False(true, indexStoragePath + " exists");
            }
        }

        private async Task ModifyDatabaseSettings(string databaseName, Action<DatabaseRecord> modifySettings)
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, databaseName);

                modifySettings(databaseRecord);

                var (etag, _) = await Server.ServerStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null);
                await Server.ServerStore.Cluster.WaitForIndexNotification(etag);
            }
        }
    }
}
