using System.Linq;
using System.Threading.Tasks;

using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Tests.Core;

using Xunit;

namespace FastTests.Client.Indexing
{
    public class BasicIndexing : RavenTestBase
    {
        [Fact]
        public async Task CanReset()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                var database = await Server.ServerStore.DatabasesLandlord.GetResourceInternal(store.DefaultDatabase).ConfigureAwait(false);

                var indexId = database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
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
                var database = await Server.ServerStore.DatabasesLandlord.GetResourceInternal(store.DefaultDatabase).ConfigureAwait(false);

                var indexId = database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
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
                var database = await Server.ServerStore.DatabasesLandlord.GetResourceInternal(store.DefaultDatabase).ConfigureAwait(false);

                database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
                database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name2" } }));

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
    }
}