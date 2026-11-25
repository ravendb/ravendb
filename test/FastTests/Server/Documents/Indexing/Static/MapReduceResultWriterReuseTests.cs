using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class MapReduceResultWriterReuseTests : RavenTestBase
    {
        public MapReduceResultWriterReuseTests(ITestOutputHelper output) : base(output)
        {
        }

        private class Order
        {
            public string Company { get; set; }
            public bool Bad { get; set; }
        }

        private class Result
        {
            public string Company { get; set; }
            public int Count { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MapResultsAfterAnOutputWithoutTheGroupByFieldAreWrittenCorrectly(Options options)
        {
            // the writer of the map results is reused within a batch, an output that fails after its object was written
            // (missing group by field) must not leave its state to the map results of the next documents in the batch
            using var store = GetDocumentStore(options);

            // few failures, so the index isn't marked as errored
            var orders = Enumerable.Range(0, 30)
                .Select(i => new Order { Company = $"companies/{i % 2}", Bad = i % 10 == 3 })
                .ToList();

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < orders.Count; i++)
                    session.Store(orders[i], $"orders/{i}");

                session.SaveChanges();
            }

            var expected = orders
                .Where(x => x.Bad == false)
                .GroupBy(x => x.Company)
                .Select(g => new Result { Company = g.Key, Count = g.Count() })
                .OrderBy(x => x.Company)
                .ToList();

            const string indexName = "Orders/ByCompany";
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps =
                {
                    "map('Orders', o => o.Bad ? { Count: 1 } : { Company: o.Company, Count: 1 })"
                },
                Reduce = "groupBy(x => x.Company).aggregate(g => ({ Company: g.key, Count: g.values.reduce((count, v) => count + v.Count, 0) }))"
            }));

            Indexes.WaitForIndexing(store, allowErrors: true);

            var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName })).SelectMany(x => x.Errors).ToList();
            Assert.Equal(orders.Count(x => x.Bad), errors.Count);
            Assert.All(errors, e => Assert.Contains("does not contain all fields that the index is supposed to group by", e.Error));

            using (var session = store.OpenSession())
            {
                var results = session.Query<Result>(indexName)
                    .ToList()
                    .OrderBy(x => x.Company)
                    .ToList();

                Assert.Equal(expected.Select(x => (x.Company, x.Count)), results.Select(x => (x.Company, x.Count)));
            }
        }
    }
}
