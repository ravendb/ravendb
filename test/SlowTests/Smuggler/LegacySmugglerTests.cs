using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Smuggler;
using Xunit;

namespace SlowTests.Smuggler
{
    public class LegacySmugglerTests : RavenTestBase
    {
        [Theory]
        [InlineData("SlowTests.Smuggler.Northwind_3.5.35168.ravendbdump")]
        public async Task CanImportNorthwind(string file)
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            {
                Assert.NotNull(stream);

                using (var store = GetDocumentStore())
                {
                    await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), stream);

                    var stats = store.DatabaseCommands.GetStatistics();

                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfIndexes); // there are 4 in ravendbdump, but Raven/DocumentsByEntityName is skipped
                    Assert.Equal(1, stats.CountOfTransformers);
                }
            }
        }

        [Theory]
        [InlineData("SlowTests.Smuggler.Indexes_And_Transformers_3.5.ravendbdump")]
        public async Task CanImportIndexesAndTransformers(string file)
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream(file))
            {
                Assert.NotNull(stream);

                using (var store = GetDocumentStore())
                {
                    await store.AsyncDatabaseCommands.Admin.StopIndexingAsync();

                    await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), stream);

                    var stats = store.DatabaseCommands.GetStatistics();

                    Assert.Equal(0, stats.CountOfDocuments);

                    // not everything can be imported
                    // LoadDocument(key)
                    // Spatial
                    Assert.True(stats.CountOfIndexes >= 589);
                    Assert.True(stats.CountOfIndexes <= 658);

                    // not everything can be imported
                    // LoadDocument(key)
                    // Query
                    // QueryOrDefault
                    Assert.True(stats.CountOfTransformers >= 71);
                    Assert.True(stats.CountOfTransformers <= 121);
                }
            }
        }
    }
}