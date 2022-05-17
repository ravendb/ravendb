using Tests.Infrastructure;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class CreateSampleDataTests : RavenTestBase
    {
        public CreateSampleDataTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CreateSampleData(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.TimeSeries | DatabaseItemType.RevisionDocuments | DatabaseItemType.Documents));
                var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.NotNull(record.Revisions);
                Assert.True(record.Revisions.Collections.ContainsKey("Orders"));
                Assert.Equal(false, record.Revisions.Collections["Orders"].Disabled);
                Assert.NotNull(record.TimeSeries?.NamedValues);
                Assert.Contains("Companies", record.TimeSeries.NamedValues.Keys);
                Assert.Contains("Employees", record.TimeSeries.NamedValues.Keys);

                Assert.NotNull(stats);
                Assert.Equal(9, stats.Collections.Count);
                Assert.Equal(1059, stats.CountOfDocuments);
                var expectedCollections = new string[] { "Shippers", "Suppliers", "Orders", "Regions", "Categories", "Companies", "Employees", "Products", "@hilo" };
                foreach (var collection in expectedCollections)
                {
                    Assert.True(stats.Collections.ContainsKey(collection), $"collection {collection} missing from created sample data");
                }

                var res = await Assert.ThrowsAnyAsync<RavenException>(async () => await store.Maintenance.SendAsync(new CreateSampleDataOperation()));
                Assert.Contains("You cannot create sample data in a database that already contains documents", res.Message);
            }
        }
    }
}
