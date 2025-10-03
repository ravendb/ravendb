using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3300 : RavenTestBase
    {
        public RavenDB_3300(ITestOutputHelper output) : base(output)
        {
        }

        private class Car
        {
            public String Model { get; set; }
            public String Color { get; set; }
            public int Year { get; set; }

        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExposeResultEtagInStatistics(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<Car>()
                        .Statistics(out stats)
                        .Where(x => x.Color == "Blue")
                        .ToList();
                    var resultEtag = stats.ResultEtag;
                    Assert.NotNull(resultEtag);
                    Assert.NotEqual(resultEtag, 0);
                }
            }
        }
    }
}
