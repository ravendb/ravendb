using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5610 : RavenLowLevelTestBase
    {
        [Fact]
        public async Task UpdateType()
        {
            using (var database = CreateDocumentDatabase())
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                var etag = await database.IndexStore.CreateIndex(indexDefinition);
                Assert.True(etag > 0);

                var index = database.IndexStore.GetIndex(etag);

                var options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Noop, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = "somePath";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Update, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = "somePath";
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Update, options);
            }
        }

        [Fact]
        public async Task WillUpdate()
        {


            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                var etag = await database.IndexStore.CreateIndex(indexDefinition);
                Assert.True(etag > 0);

                var index = database.IndexStore.GetIndex(etag);
                Assert.Equal(33, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                etag = await database.IndexStore.CreateIndex(indexDefinition);
                Assert.True(etag > 0);

                index = database.IndexStore.GetIndex(etag);
                Assert.Equal(30, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);

                var indexName = index.Name;

                Server.ServerStore.DatabasesLandlord.UnloadDatabase(database.Name);

                database = await GetDatabase(database.Name);

                index = database.IndexStore.GetIndex(indexName);
                Assert.Equal(30, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);
            }
        }

        private static IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Name = "Users_ByName",
                Maps = { "from user in docs.Users select new { user.Name }" },
                Type = IndexType.Map
            };
        }
    }
}