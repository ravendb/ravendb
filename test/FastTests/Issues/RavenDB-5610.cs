using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_5610 : RavenLowLevelTestBase
    {
        public RavenDB_5610(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task UpdateType()
        {
            using (var database = CreateDocumentDatabase())
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                var index = await database.IndexStore.CreateIndex(indexDefinition, Guid.NewGuid().ToString());
                Assert.NotNull(index);

                var options = IndexStore.GetIndexCreationOptions(indexDefinition, index.Instance.ToIndexInformationHolder(), database.Configuration, out var _);
                Assert.Equal(IndexCreationOptions.Noop, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = IndexStore.GetIndexCreationOptions(indexDefinition, index.Instance.ToIndexInformationHolder(), database.Configuration, out var _);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle)] = "10";

                options = IndexStore.GetIndexCreationOptions(indexDefinition, index.Instance.ToIndexInformationHolder(), database.Configuration, out var _);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle)] = "20";
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = IndexStore.GetIndexCreationOptions(indexDefinition, index.Instance.ToIndexInformationHolder(), database.Configuration, out var _);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);
            }
        }

        [Fact]
        public async Task WillUpdate()
        {
            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                var index = (await database.IndexStore.CreateIndex(indexDefinition, Guid.NewGuid().ToString())).Instance;
                Assert.NotNull(index);
                Assert.Equal(33, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                index = (await database.IndexStore.CreateIndex(indexDefinition, Guid.NewGuid().ToString())).Instance;
                Assert.NotNull(index);
                Assert.Equal(30, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);

                var indexName = index.Name;

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(database.Name);

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
