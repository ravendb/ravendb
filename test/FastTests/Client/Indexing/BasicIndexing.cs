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
    }
}