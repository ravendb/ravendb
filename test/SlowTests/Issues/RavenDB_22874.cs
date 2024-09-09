using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22874 : ReplicationTestBase
    {
        public RavenDB_22874(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Indexes)]
        public async Task ArtificialTombstonesShouldNotBeReplicated()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                await source.Maintenance.SendAsync(new CreateSampleDataOperation());

                var index = new Index_Orders_ByCompany();

                await index.ExecuteAsync(source);
                Indexes.WaitForIndexing(source);

                await SetupReplicationAsync(source, destination);
                await EnsureReplicatingAsync(source, destination);

                await index.ExecuteAsync(destination);
                Indexes.WaitForIndexing(destination);

                var stats1 = await source.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                var stats2 = await destination.Maintenance.SendAsync(new GetCollectionStatisticsOperation());

                Assert.True(stats1.Collections.TryGetValue("Outputs", out var outputs1));
                Assert.True(stats2.Collections.TryGetValue("Outputs", out var outputs2));
                Assert.Equal(outputs1, outputs2);

                string id = null;
                var database = await GetDatabase(source.Database);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var output = database.DocumentsStorage.GetDocumentsFrom(context, "Outputs", etag: 0, start: 0, take: 1).ToList();
                    Assert.NotEmpty(output);

                    id = output[0].Id;
                }

                using (var session = source.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(source, destination);

                stats1 = await source.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                stats2 = await destination.Maintenance.SendAsync(new GetCollectionStatisticsOperation());

                Assert.True(stats1.Collections.TryGetValue("Outputs", out var outputsAfter1));
                Assert.True(stats2.Collections.TryGetValue("Outputs", out var outputsAfter2));
                Assert.Equal(outputsAfter1, outputs1 - 1);
                Assert.Equal(outputsAfter2, outputs2);
            }
        }

        private class Index_Orders_ByCompany : AbstractIndexCreationTask
        {
            public override string IndexName => "Orders/ByCompany";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from order in docs.Orders
                           select new
                            {
                                order.Company,
                                Count = 1,
                                Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                        }"
                    },
                    Reduce = @"from result in results
                                group result by result.Company 
                                into g
                                select new
                                {
	                                Company = g.Key,
	                                Count = g.Sum(x => x.Count),
	                                Total = g.Sum(x => x.Total)
                                };",

                    OutputReduceToCollection = "Outputs",
                    PatternReferencesCollectionName = "Outputs/{Company}"
                };
            }
        }
    }
}
