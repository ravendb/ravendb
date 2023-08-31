using FastTests;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17420 : RavenTestBase
    {
        public RavenDB_17420(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Name;
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_use_boost_on_in_query(Options options)
        {
            if (options.DatabaseMode is RavenDatabaseMode.Sharded)
            {
                options.ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)] = false.ToString();
                };
            }
            using var store = GetDocumentStore(options);

            using (var s = store.OpenSession())
            {
                s.Store(new Item{Name = "ET"});
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                Item first = s.Advanced. DocumentQuery<Item>()
                    .WhereIn(x=>x.Name, new[]{"ET", "Alien"}).Boost(0)
                    .First();

                Assert.Equal(0, s.Advanced.GetMetadataFor(first).GetDouble("@index-score"));
            }
        }
    }
}
