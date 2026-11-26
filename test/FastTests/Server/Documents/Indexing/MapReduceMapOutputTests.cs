using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class MapReduceMapOutputTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        // A map result that cannot be written leaves the builder that the batch reuses in the middle of a document,
        // the documents after it must still be mapped correctly.
        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_results_after_a_failed_one_are_written_correctly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 30; i++)
                        session.Store(new Item { Name = "item" + i, Category = "c" + i % 2, Value = i }, "items/" + i);
                    session.SaveChanges();
                }

                // a char is not a supported map result value
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Items/ByCategory",
                    Maps = { @"from item in docs.Items select new { item.Category, Initial = item.Value % 10 == 3 ? (object)((string)item.Name)[0] : item.Name, Count = 1 }" },
                    Reduce = @"from result in results group result by result.Category into g select new { Category = g.Key, Initial = (object)null, Count = g.Sum(x => x.Count) }"
                }));

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Items/ByCategory" }));
                Assert.Equal(3, errors[0].Errors.Length);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<CategoryCount>("from index 'Items/ByCategory' order by Category").ToList();

                    Assert.Equal([("c0", 15), ("c1", 12)], results.Select(x => (x.Category, x.Count)));
                }
            }
        }

        private class Item
        {
            public string Name { get; set; }
            public string Category { get; set; }
            public int Value { get; set; }
        }

        private class CategoryCount
        {
            public string Category { get; set; }
            public int Count { get; set; }
        }

    }
}
