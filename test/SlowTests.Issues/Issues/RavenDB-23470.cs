using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23470(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void AddingOrderByScoreClauseDoesNotChangeNumberOfResults()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new TestDocument()
        {
            Name = "First",
            Vector = [[0.1f, 0.1f], [0.15f, 0.15f]]
        });

        session.Store(new TestDocument()
        {
            Name = "Second",
            Vector = [[0.25f, 0.2f], [0.25f, 0.2f]]
        });

        session.SaveChanges();


        var queryWithoutOrderBy = session.Query<TestDocument>()
            .Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(s => s.Vector),
                v => v.ByEmbedding([0.1f, 0.1f]), minimumSimilarity: 0.001f)
            .ToList();

        var queryWithOrderBy = session.Query<TestDocument>()
            .Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(f => f.WithEmbedding(s => s.Vector),
                v => v.ByEmbedding([0.1f, 0.1f]), minimumSimilarity: 0.001f)
            .OrderByScore()
            .ToList();

        Assert.Equal(queryWithOrderBy.Count, queryWithoutOrderBy.Count);
        Assert.Equal(2, queryWithOrderBy.Count);
        Assert.Equal(queryWithOrderBy.Select(x => x.Name), queryWithoutOrderBy.Select(x => x.Name));
        Assert.Equal("First", queryWithoutOrderBy[0].Name);
        Assert.Equal("Second", queryWithoutOrderBy[1].Name);
    }

    private class TestDocument
    {
        public float[][] Vector { get; set; }
        public string Name { get; set; }
    }
}
