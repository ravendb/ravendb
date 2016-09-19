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
    }
}