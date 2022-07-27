using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5514 : RavenTestBase
    {
        public RavenDB_5514(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void ShouldReportCorrectErrorWhenUsingTooManyBooleanClausesIsThrown(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 1040; i++)
                    {
                        var id = "orders/" + i + "-A";
                        bulk.Store(new Order
                        {
                            Name = id
                        }, id);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() =>
                    {
                        var q = session.Advanced.DocumentQuery<Order>()
                            .WaitForNonStaleResults()
                            .WhereLucene("Name", string.Join(" OR ", Enumerable.Range(0, 1040).Select(i => "Name:" + i)))
                            .ToList();
                    });
                    Assert.Contains("maxClauseCount is set to", e.Message);
                }
            }
        }

        private class Order
        {
            public string Name { get; set; }
        }
    }
}
