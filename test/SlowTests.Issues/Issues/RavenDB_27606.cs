using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27606 : RavenTestBase
{
    public RavenDB_27606(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void BoostedQueryNotOrderedByScoreReportsNoScore()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)] = false.ToString();
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };

        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a" });
            session.Store(new Item { Name = "b" });
            session.SaveChanges();
        }

        new Items_ByName().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var scored = session.Advanced.RawQuery<Item>("from index 'Items/ByName' where boost(Name = 'a', 10) or Name = 'b' order by score()").ToList();
            Assert.All(scored, x => Assert.True(session.Advanced.GetMetadataFor(x).ContainsKey(Constants.Documents.Metadata.IndexScore)));
        }

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<Item>("from index 'Items/ByName' where boost(Name = 'a', 10) or Name = 'b'").ToList();

            Assert.Equal(2, results.Count);
            Assert.All(results, x => Assert.False(session.Advanced.GetMetadataFor(x).ContainsKey(Constants.Documents.Metadata.IndexScore)));
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Items_ByName : AbstractIndexCreationTask<Item>
    {
        public Items_ByName()
        {
            Map = items => from item in items
                           select new { item.Name };
        }
    }
}
