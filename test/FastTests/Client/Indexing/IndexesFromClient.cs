using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Raven.Tests.Core;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class IndexesFromClient : RavenTestBase
    {
        [Fact]
        public async Task CanReset()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase).ConfigureAwait(false);

                var indexId = database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
                var index = database.IndexStore.GetIndex(indexId);

                var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();
                Assert.Equal(1, indexes.Count);

                await store.AsyncDatabaseCommands.ResetIndexAsync(index.Name).ConfigureAwait(false);

                indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();
                Assert.Equal(1, indexes.Count);
                Assert.NotEqual(indexes[0].IndexId, indexId);
            }
        }

        [Fact]
        public async Task CanDelete()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase).ConfigureAwait(false);

                var indexId = database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
                var index = database.IndexStore.GetIndex(indexId);

                var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();
                Assert.Equal(1, indexes.Count);

                await store.AsyncDatabaseCommands.DeleteIndexAsync(index.Name).ConfigureAwait(false);

                indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();
                Assert.Equal(0, indexes.Count);
            }
        }

        [Fact]
        public async Task CanStopAndStart()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase).ConfigureAwait(false);

                database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
                database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new IndexField { Name = "Name2" } }));

                var statuses = await store.AsyncDatabaseCommands.Admin.GetIndexesStatus().ConfigureAwait(false);

                Assert.Equal(2, statuses.Length);
                Assert.Equal("Running", statuses[0].Status);
                Assert.Equal("Running", statuses[1].Status);

                await store.AsyncDatabaseCommands.Admin.StopIndexingAsync().ConfigureAwait(false);

                statuses = await store.AsyncDatabaseCommands.Admin.GetIndexesStatus().ConfigureAwait(false);

                Assert.Equal(2, statuses.Length);
                Assert.Equal("Paused", statuses[0].Status);
                Assert.Equal("Paused", statuses[1].Status);

                await store.AsyncDatabaseCommands.Admin.StartIndexingAsync().ConfigureAwait(false);

                statuses = await store.AsyncDatabaseCommands.Admin.GetIndexesStatus().ConfigureAwait(false);

                Assert.Equal(2, statuses.Length);
                Assert.Equal("Running", statuses[0].Status);
                Assert.Equal("Running", statuses[1].Status);

                await store.AsyncDatabaseCommands.Admin.StopIndexAsync(statuses[1].Name).ConfigureAwait(false);

                statuses = await store.AsyncDatabaseCommands.Admin.GetIndexesStatus().ConfigureAwait(false);

                Assert.Equal(2, statuses.Length);
                Assert.Equal("Running", statuses[0].Status);
                Assert.Equal("Paused", statuses[1].Status);

                await store.AsyncDatabaseCommands.Admin.StartIndexAsync(statuses[1].Name).ConfigureAwait(false);

                statuses = await store.AsyncDatabaseCommands.Admin.GetIndexesStatus().ConfigureAwait(false);

                Assert.Equal(2, statuses.Length);
                Assert.Equal("Running", statuses[0].Status);
                Assert.Equal("Running", statuses[1].Status);
            }
        }

        [Fact]
        public async Task GetStats()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }).ConfigureAwait(false);
                    await session.StoreAsync(new User { Name = "Arek" }).ConfigureAwait(false);

                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                using (var session = store.OpenSession())
                {
                    var users = session
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    Assert.Equal(1, users.Count);
                }

                var indexes = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 128).ConfigureAwait(false);
                Assert.Equal(1, indexes.Length);

                var index = indexes[0];
                var request = store.AsyncDatabaseCommands.CreateRequest("/indexes/stats?name=" + index.Name, HttpMethod.Get);
                var json = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                var stats = json.JsonDeserialization<IndexStats>();

                Assert.Equal(index.IndexId, stats.Id);
                Assert.Equal(index.Name, stats.Name);
                Assert.False(stats.IsInvalidIndex);
                Assert.False(stats.IsTestIndex);
                Assert.True(stats.IsInMemory);
                Assert.Equal(IndexType.AutoMap, stats.Type);
                Assert.Equal(2, stats.EntriesCount);
                Assert.Equal(2, stats.IndexingAttempts);
                Assert.Equal(0, stats.IndexingErrors);
                Assert.Equal(2, stats.IndexingSuccesses);
                Assert.Equal(1, stats.ForCollections.Length);
                Assert.Equal(2 + 1, stats.LastIndexedEtags[stats.ForCollections[0]]); // +1 because of HiLo
                Assert.True(stats.LastIndexingTime.HasValue);
                Assert.True(stats.LastQueryingTime.HasValue);
                Assert.Equal(IndexLockMode.Unlock, stats.LockMode);
                Assert.Equal(IndexingPriority.Normal, stats.Priority);
            }
        }

        [Fact]
        public async Task SetLockModeAndSetPriority()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }).ConfigureAwait(false);
                    await session.StoreAsync(new User { Name = "Arek" }).ConfigureAwait(false);

                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                using (var session = store.OpenSession())
                {
                    var users = session
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    Assert.Equal(1, users.Count);
                }

                var indexes = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 128).ConfigureAwait(false);
                Assert.Equal(1, indexes.Length);

                var index = indexes[0];
                var request = store.AsyncDatabaseCommands.CreateRequest("/indexes/stats?name=" + index.Name, HttpMethod.Get);
                var json = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                var stats = json.JsonDeserialization<IndexStats>();

                Assert.Equal(index.IndexId, stats.Id);
                Assert.Equal(IndexLockMode.Unlock, stats.LockMode);
                Assert.Equal(IndexingPriority.Normal, stats.Priority);

                await store.AsyncDatabaseCommands.SetIndexLockAsync(index.Name, IndexLockMode.LockedIgnore).ConfigureAwait(false);
                await store.AsyncDatabaseCommands.SetIndexPriorityAsync(index.Name, IndexingPriority.Error).ConfigureAwait(false);

                request = store.AsyncDatabaseCommands.CreateRequest("/indexes/stats?name=" + index.Name, HttpMethod.Get);
                json = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                stats = json.JsonDeserialization<IndexStats>();

                Assert.Equal(index.IndexId, stats.Id);
                Assert.Equal(IndexLockMode.LockedIgnore, stats.LockMode);
                Assert.Equal(IndexingPriority.Error, stats.Priority);
            }
        }

        [Fact]
        public async Task GetErrors()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }).ConfigureAwait(false);
                    await session.StoreAsync(new User { Name = "Arek" }).ConfigureAwait(false);

                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                using (var session = store.OpenSession())
                {
                    var users = session
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    Assert.Equal(1, users.Count);
                }

                var database = await Server.ServerStore.DatabasesLandlord
                    .TryGetOrCreateResourceStore(new StringSegment(store.DefaultDatabase, 0))
                    .ConfigureAwait(false);

                var index = database.IndexStore.GetIndexes().First();
                var now = SystemTime.UtcNow;
                var nowNext = now.AddTicks(1);

                var batchStats = new IndexingBatchStats();
                batchStats.AddMapError("users/1", "error/1");
                batchStats.AddAnalyzerError(new IndexAnalyzerException());
                batchStats.Errors[0].Timestamp = now;
                batchStats.Errors[1].Timestamp = nowNext;

                index._indexStorage.UpdateStats(SystemTime.UtcNow, batchStats);

                var error = await store.AsyncDatabaseCommands.GetIndexErrorsAsync(index.Name).ConfigureAwait(false);
                Assert.Equal(index.Name, error.Name);
                Assert.Equal(2, error.Errors.Length);
                Assert.Equal("Map", error.Errors[0].Action);
                Assert.Equal("users/1", error.Errors[0].Document);
                Assert.Equal("error/1", error.Errors[0].Error);
                Assert.Equal(now, error.Errors[0].Timestamp);

                Assert.Equal("Analyzer", error.Errors[1].Action);
                Assert.Null(error.Errors[1].Document);
                Assert.True(error.Errors[1].Error.Contains("Could not create analyzer:"));
                Assert.Equal(nowNext, error.Errors[1].Timestamp);

                var errors = await store.AsyncDatabaseCommands.GetIndexErrorsAsync().ConfigureAwait(false);
                Assert.Equal(1, errors.Length);

                errors = await store.AsyncDatabaseCommands.GetIndexErrorsAsync(new[] { index.Name }).ConfigureAwait(false);
                Assert.Equal(1, errors.Length);

                var stats = await store.AsyncDatabaseCommands.GetIndexStatisticsAsync(index.Name).ConfigureAwait(false);
                Assert.Equal(2, stats.ErrorsCount);
            }
        }

        [Fact]
        public async Task GetDefinition()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }).ConfigureAwait(false);
                    await session.StoreAsync(new User { Name = "Arek" }).ConfigureAwait(false);

                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                using (var session = store.OpenSession())
                {
                    var users = session
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    Assert.Equal(1, users.Count);
                }

                var database = await Server.ServerStore.DatabasesLandlord
                    .TryGetOrCreateResourceStore(new StringSegment(store.DefaultDatabase, 0))
                    .ConfigureAwait(false);

                var index = database.IndexStore.GetIndexes().First();
                var serverDefinition = index.GetIndexDefinition();

                var definition = await store.AsyncDatabaseCommands.GetIndexAsync("do-not-exist").ConfigureAwait(false);
                Assert.Null(definition);

                definition = await store.AsyncDatabaseCommands.GetIndexAsync(index.Name).ConfigureAwait(false);
                Assert.Equal(serverDefinition.Name, definition.Name);
                Assert.Equal(serverDefinition.IsSideBySideIndex, definition.IsSideBySideIndex);
                Assert.Equal(serverDefinition.IsTestIndex, definition.IsTestIndex);
                Assert.Equal(serverDefinition.IndexVersion, definition.IndexVersion);
                Assert.Equal(serverDefinition.Reduce, definition.Reduce);
                Assert.Equal(serverDefinition.Type, definition.Type);
                Assert.Equal(serverDefinition.IndexId, definition.IndexId);
                Assert.Equal(serverDefinition.LockMode, definition.LockMode);
                Assert.Equal(serverDefinition.MaxIndexOutputsPerDocument, definition.MaxIndexOutputsPerDocument);
                Assert.Equal(serverDefinition.Maps, definition.Maps);

                var keys = serverDefinition.Fields.Keys;
                foreach (var key in keys)
                {
                    var serverField = serverDefinition.Fields[key];
                    var field = definition.Fields[key];
                    
                    Assert.Equal(serverField.Indexing, field.Indexing);
                    Assert.Equal(serverField.Analyzer, field.Analyzer);
                    Assert.Equal(serverField.Sort, field.Sort);
                    Assert.Equal(serverField.Spatial, field.Spatial);
                    Assert.Equal(serverField.Storage, field.Storage);
                    Assert.Equal(serverField.Suggestions, field.Suggestions);
                    Assert.Equal(serverField.TermVector, field.TermVector);
                }

                var definitions = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 128).ConfigureAwait(false);
                Assert.Equal(1, definitions.Length);
                Assert.Equal(index.Name, definitions[0].Name);
            }
        }
    }
}