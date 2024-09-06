﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
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
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;
using Index = Raven.Server.Documents.Indexes.Index;

namespace FastTests.Server.Documents.Indexing.Auto
{
    public class BasicAutoMapIndexing : RavenLowLevelTestBase
    {
        public BasicAutoMapIndexing(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenExplicitData]
        public async Task CheckDispose(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
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
        
        [Theory]
        [RavenExplicitData]
        public async Task CanPersist(RavenTestParameters config)
        {
            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database, modifyConfiguration: dictionary => dictionary[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString()))
            {
                var dbName = database.Name;
                var name1 = new AutoIndexField
                {
                    Name = "Name1",
                    Storage = FieldStorage.No,
                };
                Assert.NotNull(await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 }), Guid.NewGuid().ToString()));
                var name2 = new AutoIndexField
                {
                    Name = "Name2",
                    Storage = FieldStorage.No,
                    Indexing = AutoFieldIndexing.Default | AutoFieldIndexing.Search,
                    HasQuotedName = true
                };

                var index2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name2 }), Guid.NewGuid().ToString());
                Assert.NotNull(index2);

                var task1 = database.IndexStore.SetLock(index2.Name, IndexLockMode.LockedError, Guid.NewGuid().ToString());
                var task2 = database.IndexStore.SetPriority(index2.Name, IndexPriority.Low, Guid.NewGuid().ToString());
                index2.SetState(IndexState.Disabled);
                var task = Task.WhenAll(task1, task2);

                await Assert.ThrowsAsync<NotSupportedException>(async () => await task);
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", task1.Exception.InnerException.Message);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                database = await GetDatabase(dbName);

                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x => x.Name.Length)
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Equal(1, indexes[0].Definition.Collections.Count);
                Assert.Equal("Users", indexes[0].Definition.Collections.Single());
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Equal("Name1", indexes[0].Definition.MapFields["Name1"].Name);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Definition.Priority);
                Assert.Equal(IndexState.Normal, indexes[0].State);
                Assert.Equal(config.SearchEngine.ToString(), index2.SearchEngineType.ToString());
                
                Assert.Equal(1, indexes[1].Definition.Collections.Count);
                Assert.Equal("Users", indexes[1].Definition.Collections.Single());
                Assert.Equal(1, indexes[1].Definition.MapFields.Count);
                Assert.Equal("Name2", indexes[1].Definition.MapFields["Name2"].Name);
                Assert.True(((AutoIndexField)indexes[1].Definition.MapFields["Name2"]).HasQuotedName);
                Assert.Equal(AutoFieldIndexing.Search | AutoFieldIndexing.Default, indexes[1].Definition.MapFields["Name2"].As<AutoIndexField>().Indexing);
                Assert.Equal(IndexLockMode.Unlock, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.Low, indexes[1].Definition.Priority);
                Assert.Equal(IndexState.Disabled, indexes[1].State);
            }
        }

        [Theory]
        [RavenExplicitData]
        public async Task CanDelete(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabase(modifyConfiguration: dictionary => dictionary[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString()))
                await CanDeleteInternal(database);

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database,modifyConfiguration: dictionary => dictionary[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString()))
                await CanDeleteInternal(database);
        }

        private static async Task CanDeleteInternal(DocumentDatabase database)
        {
            var def1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name1" } });
            var index1 =
                await database.IndexStore.CreateIndex(
                    def1, Guid.NewGuid().ToString());
            var path1 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(def1.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var def2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name2" } });
            var index2 =
                await database.IndexStore.CreateIndex(
                    def2, Guid.NewGuid().ToString());
            var path2 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(def2.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            Assert.Equal(2, database.IndexStore.GetIndexesForCollection("Users").Count());

            await database.IndexStore.DeleteIndex(index1.Name, Guid.NewGuid().ToString());

            if (index1.Configuration.RunInMemory == false)
                Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path1) == false, TimeSpan.FromSeconds(5)));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(1, indexes.Count);

            await database.IndexStore.DeleteIndex(index2.Name, Guid.NewGuid().ToString());

            if (index1.Configuration.RunInMemory == false)
                Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(0, indexes.Count);
        }

        [Theory]
        [RavenExplicitData]
        public async Task CanReset(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
                await CanResetInternal(database);

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database, modifyConfiguration: dictionary => GetModificationDictionaryForSearchEngine(dictionary, config)))
                await CanResetInternal(database);
        }

        private static async Task CanResetInternal(DocumentDatabase database)
        {
            var def1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name1" } });
            var index1 = await database.IndexStore.CreateIndex(def1, Guid.NewGuid().ToString());

            var path1 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(def1.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var def2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name2" } });
            var index2 = await database.IndexStore.CreateIndex(def2, Guid.NewGuid().ToString());
            var path2 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(def2.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            var indexesAfterReset = database.IndexStore.GetIndexesForCollection("Users");
            Assert.Equal(2, indexesAfterReset.Count());

            var index3 = database.IndexStore.ResetIndex(index1.Name);
            var path3 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath, IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(def1.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path3));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);

            var index4 = database.IndexStore.ResetIndex(index2.Name);
            var path4 = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath, IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(def2.Name));

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path4));

            //Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);
        }

        [Theory]
        [RavenExplicitData]
        public void SimpleIndexing(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
            {
                using (var index = AutoMapIndex.CreateNew(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField
                {
                    Name = "Name",
                    Id = 1,
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
                        Dictionary<string, LastTombstoneInfo> lastProcessedTombstonesPerCollection = index.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);
                        Assert.Equal(4, lastProcessedTombstonesPerCollection.Select(ltpc => ltpc.Value.Etag).Prepend(long.MaxValue).Min());
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

        [Theory]
        [RavenExplicitData]
        public void WriteErrors(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
            {
                using (var index = AutoMapIndex.CreateNew(
                    new AutoMapIndexDefinition(
                        "Users",
                        new[] { new AutoIndexField { Name = "Name", Id = 1, Storage = FieldStorage.No } }),
                    database))
                {
                    var mre = new ManualResetEvent(false);

                    database.IndexStore.IndexBatchCompleted = x => { mre.Set(); };

                    index.Start();
                    Assert.Equal(IndexRunningStatus.Running, index.Status);

                    Assert.True(mre.WaitOne(TimeSpan.FromSeconds(15)));

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

        [Theory]
        [RavenExplicitData]
        public void Errors2(RavenTestParameters config)
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
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
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

        [Theory]
        [RavenExplicitData]
        public void Errors(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
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

        [Theory]
        [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task AutoIndexesShouldBeMarkedAsIdleAndDeleted(RavenTestParameters config)
        {
            void WaitForIndexDeletion(DocumentDatabase database, string indexName)
            {
                Assert.True(SpinWait.SpinUntil(() => database.IndexStore.GetIndex(indexName) == null, TimeSpan.FromSeconds(15)));
            }

            void WaitForIndex(DocumentDatabase database, string indexName, Func<Index, bool> condition)
            {
                Assert.True(SpinWait.SpinUntil(() => condition(database.IndexStore.GetIndex(indexName)), TimeSpan.FromSeconds(15)));
            }

            DoNotReuseServer();
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
            {
                Assert.Equal(config.SearchEngine.ToString(), database.Configuration.Indexing.AutoIndexingEngineType.ToString());
                var index0 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Job", Storage = FieldStorage.No, Id = 1 } }), Guid.NewGuid().ToString());

                await database.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index0.Name, IndexState.Idle, database.Name, Guid.NewGuid().ToString()));

                var now = database.Time.GetUtcNow();
                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name, out _))
                {
                    var state = new ClusterObserver.DatabaseObservationState
                    {
                        RawDatabase = rawRecord,
                        Name = database.Name,
                        DatabaseTopology = rawRecord.Topology,
                        Current = new Dictionary<string, ClusterNodeStatusReport>
                        {
                            {
                                Server.ServerStore.NodeTag, new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>
                                    {
                                        {
                                            database.Name, new DatabaseStatusReport
                                            {
                                                UpTime =
                                                    database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan.Add(TimeSpan.FromSeconds(1)),
                                                LastIndexStats = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
                                                {
                                                    {
                                                        index0.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Idle,
                                                            LastQueried = database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    ClusterNodeStatusReport.ReportStatus.Ok,
                                    null,
                                    now,
                                    null)
                            }
                        }
                    };

                    await CleanupUnusedAutoIndexes(state);
                }

                WaitForIndexDeletion(database, index0.Name);

                var index1 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Companies", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No, Id = 1 } }), Guid.NewGuid().ToString());
                var index2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Age", Storage = FieldStorage.No, Id = 1 } }), Guid.NewGuid().ToString());
                using (var context = QueryOperationContext.ShortTermSingleUse(database))
                {
                    await index1.Query(new IndexQueryServerSide("FROM Companies"), context, OperationCancelToken.None); // last querying time
                }
                using (var context = QueryOperationContext.ShortTermSingleUse(database))
                {
                    await index2.Query(new IndexQueryServerSide("FROM Users"), context, OperationCancelToken.None); // last querying time
                }

                Assert.Equal(IndexState.Normal, index1.State);
                Assert.Equal(IndexState.Normal, index2.State);

                now = database.Time.GetUtcNow();

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name, out _))
                {
                    var state = new ClusterObserver.DatabaseObservationState()
                    {
                        RawDatabase = rawRecord,
                        DatabaseTopology = rawRecord.Topology,
                        Name = database.Name,
                        Current = new Dictionary<string, ClusterNodeStatusReport>
                        {
                            {
                                Server.ServerStore.NodeTag, new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>
                                    {
                                        {
                                            database.Name, new DatabaseStatusReport
                                            {
                                                UpTime = now - database.StartTime,
                                                LastIndexStats = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
                                                {
                                                    {
                                                        index1.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Normal,
                                                            LastQueried = now - index1.GetLastQueryingTime()
                                                        }
                                                    },
                                                    {
                                                        index2.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Normal,
                                                            LastQueried = now - index2.GetLastQueryingTime()
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    ClusterNodeStatusReport.ReportStatus.Ok,
                                    null,
                                    now,
                                    null)
                            }
                        }
                    };

                    // nothing should happen because difference between querying time between those two indexes is less than TimeToWaitBeforeMarkingAutoIndexAsIdle
                    await CleanupUnusedAutoIndexes(state);
                }

                WaitForIndex(database, index1.Name, index => index.State == IndexState.Normal);
                WaitForIndex(database, index2.Name, index => index.State == IndexState.Normal);

                database.Time.UtcDateTime = () => DateTime.UtcNow.Add(database.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan);

                using (var context = QueryOperationContext.ShortTermSingleUse(database))
                {
                    await index1.Query(new IndexQueryServerSide("FROM Companies"), context, OperationCancelToken.None); // last querying time
                }

                now = database.Time.GetUtcNow();

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name, out _))
                {
                    var state = new ClusterObserver.DatabaseObservationState()
                    {
                        RawDatabase = rawRecord,
                        DatabaseTopology = rawRecord.Topology,
                        Name = database.Name,
                        Current = new Dictionary<string, ClusterNodeStatusReport>
                        {
                            {
                                Server.ServerStore.NodeTag, new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>
                                    {
                                        {
                                            database.Name, new DatabaseStatusReport
                                            {
                                                UpTime = now - database.StartTime,
                                                LastIndexStats = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
                                                {
                                                    {
                                                        index1.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Normal,
                                                            LastQueried = now - index1.GetLastQueryingTime()
                                                        }
                                                    },
                                                    {
                                                        index2.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Normal,
                                                            LastQueried = now - index2.GetLastQueryingTime()
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    ClusterNodeStatusReport.ReportStatus.Ok,
                                    null,
                                    now,
                                    null)
                            }
                        }
                    };

                    // this will mark index2 as idle, because the difference between two indexes and index last querying time is more than TimeToWaitBeforeMarkingAutoIndexAsIdle
                    await CleanupUnusedAutoIndexes(state);
                }

                WaitForIndex(database, index1.Name, index => index.State == IndexState.Normal);
                WaitForIndex(database, index2.Name, index => index.State == IndexState.Idle);

                await database.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index2.Name, IndexState.Idle, database.Name, Guid.NewGuid().ToString()));

                now = database.Time.GetUtcNow();
                database.Time.UtcDateTime = () =>
                        now.Add(TimeSpan.FromSeconds(1))
                           .Add(database.Configuration.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan);

                now = database.Time.GetUtcNow();

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name, out _))
                {
                    var state = new ClusterObserver.DatabaseObservationState()
                    {
                        RawDatabase = rawRecord,
                        DatabaseTopology = rawRecord.Topology,
                        Name = database.Name,
                        Current = new Dictionary<string, ClusterNodeStatusReport>
                        {
                            {
                                Server.ServerStore.NodeTag, new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>
                                    {
                                        {
                                            database.Name, new DatabaseStatusReport
                                            {
                                                UpTime = now - database.StartTime,
                                                LastIndexStats = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
                                                {
                                                    {
                                                        index1.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Normal,
                                                            LastQueried = now - index1.GetLastQueryingTime()
                                                        }
                                                    },
                                                    {
                                                        index2.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Normal,
                                                            LastQueried = now - index2.GetLastQueryingTime()
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    ClusterNodeStatusReport.ReportStatus.Ok,
                                    null,
                                    now,
                                    null)
                            }
                        }
                    };

                    // should not remove anything, age will be greater than 2x TimeToWaitBeforeMarkingAutoIndexAsIdle but less than TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle
                    await CleanupUnusedAutoIndexes(state);
                }

                WaitForIndex(database, index1.Name, index => index.State == IndexState.Idle);
                WaitForIndex(database, index2.Name, index => index.State == IndexState.Idle);

                await database.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index1.Name, IndexState.Idle, database.Name, Guid.NewGuid().ToString()));
                await database.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index2.Name, IndexState.Idle, database.Name, Guid.NewGuid().ToString()));


                now = database.Time.GetUtcNow();
                database.Time.UtcDateTime = () => now.Add(database.Configuration.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan);

                now = database.Time.GetUtcNow();

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = database.ServerStore.Cluster.ReadRawDatabaseRecord(context, database.Name, out _))
                {
                    var state = new ClusterObserver.DatabaseObservationState()
                    {
                        RawDatabase = rawRecord,
                        DatabaseTopology = rawRecord.Topology,
                        Name = database.Name,
                        Current = new Dictionary<string, ClusterNodeStatusReport>
                        {
                            {
                                Server.ServerStore.NodeTag, new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>
                                    {
                                        {
                                            database.Name, new DatabaseStatusReport
                                            {
                                                UpTime = now - database.StartTime,
                                                LastIndexStats = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
                                                {
                                                    {
                                                        index1.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Idle,
                                                            LastQueried = now - index1.GetLastQueryingTime()
                                                        }
                                                    },
                                                    {
                                                        index2.Name, new DatabaseStatusReport.ObservedIndexStatus
                                                        {
                                                            State = IndexState.Idle,
                                                            LastQueried = now - index2.GetLastQueryingTime()
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    ClusterNodeStatusReport.ReportStatus.Ok,
                                    null,
                                    now,
                                    null)
                            }
                        }
                    };

                    // this will delete indexes
                    await CleanupUnusedAutoIndexes(state);
                }

                WaitForIndexDeletion(database, index1.Name);
                WaitForIndexDeletion(database, index2.Name);
            }
        }

        private async Task CleanupUnusedAutoIndexes(ClusterObserver.DatabaseObservationState state)
        {
            var indexCleanupCommands = Server.ServerStore.Observer.GetUnusedAutoIndexes(state);
            foreach (var (cmd, _) in indexCleanupCommands)
            {
                await Server.ServerStore.Engine.SendToLeaderAsync(cmd);
            }
        }

        [Theory]
        [RavenExplicitData]
        public async Task IndexCreationOptions(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
            {
                var definition1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });
                var definition2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });

                var index1 = await database.IndexStore.CreateIndex(definition1, Guid.NewGuid().ToString());
                var index2 = await database.IndexStore.CreateIndex(definition2, Guid.NewGuid().ToString());

                Assert.Equal(index1, index2);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());

                var definition3 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.Yes } });

                var e = await Assert.ThrowsAsync<IndexCreationException>(() => database.IndexStore.CreateIndex(definition3, Guid.NewGuid().ToString()));

                Assert.Contains("Can not update auto-index", e.ToString());
                Assert.NotNull(index1);
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());
            }
        }

        [Theory]
        [RavenExplicitData]
        public async Task LockMode(RavenTestParameters config)
        {
            using (var database = CreateDocumentDatabaseForSearchEngine(config))
            {
                var definition1 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });
                var definition2 = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No } });

                var index1 = await database.IndexStore.CreateIndex(definition1, Guid.NewGuid().ToString());

                var exception = Assert.Throws<NotSupportedException>(() => index1.SetLock(IndexLockMode.LockedIgnore));
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", exception.Message);

                await database.IndexStore.CreateIndex(definition2, Guid.NewGuid().ToString());
                Assert.Equal(1, database.IndexStore.GetIndexes().Count());

                exception = Assert.Throws<NotSupportedException>(() => index1.SetLock(IndexLockMode.LockedError));
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", exception.Message);

                var index2 = await database.IndexStore.CreateIndex(definition2, Guid.NewGuid().ToString());

                Assert.NotNull(index1);
                Assert.NotNull(index2);
            }
        }

        [Theory]
        [RavenExplicitData]
        public async Task IndexLoadErrorCreatesFaultyInMemoryIndexFakeAndAddsAlert(RavenTestParameters config)
        {
            string indexStoragePath;
            string indexName;
            string dbName;

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database, modifyConfiguration: dictionary => GetModificationDictionaryForSearchEngine(dictionary, config)))
            {
                dbName = database.Name;
                var name1 = new AutoIndexField
                {
                    Name = "Name1",
                    Storage = FieldStorage.No,
                };

                var index = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 }), Guid.NewGuid().ToString());
                Assert.NotNull(index);
                indexName = index.Name;

                indexStoragePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                    IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(indexName));

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

        [Theory]
        [RavenExplicitData]
        public async Task CanDeleteFaultyIndex(RavenTestParameters config)
        {
            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database, modifyConfiguration: dictionary => GetModificationDictionaryForSearchEngine(dictionary, config)))
            {
                var dbName = database.Name;
                var name1 = new AutoIndexField
                {
                    Name = "Name1",
                    Storage = FieldStorage.No,
                };

                var index = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { name1 }), Guid.NewGuid().ToString());
                Assert.NotNull(index);
                var indexSafeName = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(index.Name);

                var indexStoragePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath,
                    IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(index.Name));


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
                Assert.Equal(indexSafeName, IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(index.Name));

                await database.IndexStore.DeleteIndex(index.Name, Guid.NewGuid().ToString());

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

                var (etag, _) = await Server.ServerStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null, Guid.NewGuid().ToString());
                await Server.ServerStore.Cluster.WaitForIndexNotification(etag);
            }
        }

        private static void GetModificationDictionaryForSearchEngine(Dictionary<string, string> dictionary, RavenTestParameters config)
        {
            dictionary[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
        }
    }
}
