using System.Linq;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBC_285 : RavenTestBase
    {
        [Fact]
        public void CanQueryWithBoostEqZero()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.DocumentQuery<Product>("Product/Search")
                        .Search(x => x.Name, "coffee syrup")
                        .OrElse()
                        .WhereIn(x => x.Category, new[] { "categories/2-A", "categories/3-A" }).Boost(0)
                        .Take(3)
                        .OrderByScore()
                        .OrderByDescending(x => x.PricePerUnit);

                    var resultsZeroBoost = session.Advanced.DocumentQuery<Product>("Product/Search")
                        .Search(x => x.Name, "coffee syrup")
                        .OrElse()
                        .WhereIn(x => x.Category, new[] { "categories/2-A", "categories/3-A" }).Boost(0)
                        .Take(3)
                        .OrderByScore()
                        .OrderByDescending(x => x.PricePerUnit)
                        .ToList();

                    var results = session.Advanced.DocumentQuery<Product>("Product/Search")
                      .Search(x => x.Name, "coffee syrup")
                      .OrElse()
                      .WhereIn(x => x.Category, new[] { "categories/2-A", "categories/3-A" })
                      .Take(3)
                      .OrderByScore()
                      .OrderByDescending(x => x.PricePerUnit)
                      .ToList();

                    Assert.Equal(results.Count, resultsZeroBoost.Count);
                    Assert.NotEqual(results, resultsZeroBoost);
                    Assert.Equal("from index 'Product/Search' where search(Name, $p0) or boost(Category in ($p1), 0) order by score(), PricePerUnit as double desc limit $p2, $p3", q.ToString());
                }
            }
        }
    }
}
