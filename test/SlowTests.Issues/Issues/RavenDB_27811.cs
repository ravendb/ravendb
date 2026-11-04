using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27811 : RavenTestBase
{
    public RavenDB_27811(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ExactResidualMustReadTheExactField(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 300; i++)
                bulk.Store(new Item { Name = i % 2 == 0 ? "ABC" : "abc", Tag = i == 4 ? "rare" : "t" });
        }

        using var session = store.OpenSession();

        // shape the auto index: Name, exact(Name) and Tag
        session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "ABC" && x.Tag == "rare").ToList();
        session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "ABC", exact: true).ToList();

        var results = session.Advanced.DocumentQuery<Item>()
            .WaitForNonStaleResults()
            .WhereEquals(x => x.Tag, "rare")
            .AndAlso()
            .WhereEquals(x => x.Name, "abc", exact: true)
            .ToList();

        // the only 'rare' document holds 'ABC'
        Assert.Empty(results);
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ExactRangeMustNotChangeTheSortOrder(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            string[] names = ["Bob", "alice", "Carl", "dave"];
            for (int i = 0; i < 20; i++)
                session.Store(new Item2 { Name = names[i % names.Length] + i.ToString("D2") });
            session.SaveChanges();
        }

        using var querySession = store.OpenSession();

        // the auto index holds both Name and exact(Name)
        querySession.Query<Item2>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name.CompareTo("a") >= 0).OrderBy(x => x.Name).ToList();
        var results = querySession.Query<Item2>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name.CompareTo("A") >= 0, exact: true)
            .OrderBy(x => x.Name)
            .Take(3)
            .ToList();

        Assert.Equal(new[] { "alice01", "alice05", "alice09" }, results.Select(x => x.Name));
    }

    private class Item2
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
