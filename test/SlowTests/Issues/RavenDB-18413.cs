using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries.Timings;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18413 : RavenTestBase
    {
        public RavenDB_18413(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ToQueryableTimings(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    QueryTimings timings = null;
                    newSession.Advanced.DocumentQuery<User>()
                        .ToQueryable()
                        .Customize(x => x.Timings(out timings)).ToList();

                    Assert.NotNull(timings.Timings);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ToQueryableTimingsOutTimings(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    QueryTimings timings = null;
                    newSession.Advanced.DocumentQuery<User>()
                        .Timings(out var timings2)
                        .ToQueryable()
                        .Customize(x => x.Timings(out timings)).ToList();

                    Assert.NotNull(timings.Timings);
                    Assert.Same(timings, timings2);
                }
            }
        }

    }

}
