using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Smuggler
{
    public class LegacySmugglerTests : RavenNewTestBase
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

                    var stats = await store.Admin.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(1059, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfIndexes); // there are 4 in ravendbdump, but Raven/DocumentsByEntityName is skipped
                    Assert.Equal(1, stats.CountOfTransformers);

                    var collectionStats = await store.Admin.SendAsync(new GetCollectionStatisticsOperation());
                    Assert.Equal(1059, collectionStats.CountOfDocuments);
                    Assert.Equal(9, collectionStats.Collections.Count);
                    Assert.Equal(8, collectionStats.Collections["Categories"]);
                    Assert.Equal(91, collectionStats.Collections["Companies"]);
                    Assert.Equal(9, collectionStats.Collections["Employees"]);
                    Assert.Equal(830, collectionStats.Collections["Orders"]);
                    Assert.Equal(77, collectionStats.Collections["Products"]);
                    Assert.Equal(4, collectionStats.Collections["Regions"]);
                    Assert.Equal(3, collectionStats.Collections["Shippers"]);
                    Assert.Equal(29, collectionStats.Collections["Suppliers"]);
                    Assert.True(collectionStats.Collections.ContainsKey("@system"));

                    using (var session = store.OpenSession())
                    {
                        var order = session.Load<Order>("orders/1");
                        Assert.NotNull(order);

                        var metadata = session.Advanced.GetMetadataFor(order);
                        Assert.False(metadata.ContainsKey("Raven-Entity-Name"));
                        Assert.False(metadata.ContainsKey("Raven-Last-Modified"));
                        Assert.False(metadata.ContainsKey("Last-Modified"));
                    }
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
                    await store.Admin.SendAsync(new StopIndexingOperation());

                    await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), stream);

                    var stats = await store.Admin.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(0, stats.CountOfDocuments);

                    // not everything can be imported
                    // LoadDocument(key)
                    // Spatial
                    Assert.True(stats.CountOfIndexes >= 589, $"{stats.CountOfIndexes} >= 589");
                    Assert.True(stats.CountOfIndexes <= 658, $"{stats.CountOfIndexes} <= 658");

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