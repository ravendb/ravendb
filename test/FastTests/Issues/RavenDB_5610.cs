using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5610 : RavenLowLevelTestBase
    {
        [Fact]
        public void UpdateType()
        {
            using (var database = CreateDocumentDatabase())
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                Assert.Equal(1, database.IndexStore.CreateIndex(indexDefinition));

                var index = database.IndexStore.GetIndex(1);

                var options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Noop, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Update, options);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                options = database.IndexStore.GetIndexCreationOptions(indexDefinition, index);
                Assert.Equal(IndexCreationOptions.Update, options);
            }
        }

        [Fact]
        public void WillUpdate()
        {
            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "33";

                Assert.Equal(1, database.IndexStore.CreateIndex(indexDefinition));

                var index = database.IndexStore.GetIndex(1);
                Assert.Equal(33, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);

                indexDefinition = CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                Assert.Equal(1, database.IndexStore.CreateIndex(indexDefinition));

                index = database.IndexStore.GetIndex(1);
                Assert.Equal(30, index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var index = database.IndexStore.GetIndex(1);
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