using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9587 : RavenTestBase
    {
        public RavenDB_9587(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void TimingsShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "CF" });
                    session.Store(new Company { Name = "HR" });

                    session.SaveChanges();
                }

                AssertTimings(store);
                AssertTimings(store); // there was a bug when building IndexQueryServerSide and QueryMetadataCache is used
            }

            void AssertTimings(IDocumentStore store)
            {
                using (var session = store.OpenSession())
                {
                    QueryTimings timings = null;
    
                    var _ = session.Query<Company>()
                        .Customize(x =>
                        {
                            x.Timings(out timings);
                            x.NoCaching();
                        })
                        .Where(x => x.Name != "HR")
                        .ToList();

                    Assert.True(timings.DurationInMs >= 0);
                    Assert.NotNull(timings.Timings);

                    var keys = GetAllTimingKeys(timings.Timings)
                        .ToHashSet();

                    Assert.Contains(nameof(QueryTimingsScope.Names.Query), keys);

                    if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
                        Assert.Contains(nameof(QueryTimingsScope.Names.Corax), keys);
                    else
                        Assert.Contains(nameof(QueryTimingsScope.Names.Lucene), keys);

                    Assert.Contains(nameof(QueryTimingsScope.Names.Optimizer), keys);
                    Assert.Contains(nameof(QueryTimingsScope.Names.Retriever), keys);
                    Assert.Contains(nameof(QueryTimingsScope.Names.Storage), keys);
                    Assert.Contains(nameof(QueryTimingsScope.Names.Staleness), keys);

                    if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                    {
                        Assert.Contains("Shard_0", keys);
                        Assert.Contains("Shard_1", keys);
                        Assert.Contains("Shard_2", keys);
                    }
                }
            }
        }

        private static IEnumerable<string> GetAllTimingKeys(IDictionary<string, QueryTimings> timings)
        {
            if (timings == null)
                yield break;

            foreach (var kvp in timings)
            {
                yield return kvp.Key;

                foreach (var inner in GetAllTimingKeys(kvp.Value.Timings))
                    yield return inner;
            }
        }
    }
}
