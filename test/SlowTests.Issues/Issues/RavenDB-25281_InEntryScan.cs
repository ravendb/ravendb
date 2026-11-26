using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25281_InEntryScan : RavenTestBase
{
    public RavenDB_25281_InEntryScan(ITestOutputHelper output) : base(output)
    {
    }

    // Triggers entry-scan path (small seed bitmap + large IN posting list)
    // to verify IN with multiple string terms is handled correctly.
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void InClauseWithMultipleStringTerms_InAndChain_ReturnsCorrectResults(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var bulk = store.BulkInsert())
        {
            // 1 highly-selective seed doc — Tag="rare", Name="Bravo"
            bulk.Store(new Doc { Tag = "rare", Name = "Bravo" });

            // 500 filler docs with Tag="common" and Name="Alpha"
            // Makes IN's "Alpha" posting list large, so entry-scan wins the cost check.
            for (int i = 0; i < 500; i++)
                bulk.Store(new Doc { Tag = "common", Name = "Alpha" });
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        // AND chain (2 clauses): Tag == "rare"  AND  Name IN ["Alpha","Bravo"]
        // The rare doc has Name="Bravo" so the IN clause matches via the second term.
        // If entry-scan only checks the first IN term ("Alpha") via single-slot
        // StartsWith, the rare doc is dropped and we'd see 0 results.
        var results = session.Query<Doc>()
            .Where(x => x.Tag == "rare" && x.Name.In(new[] { "Alpha", "Bravo" }))
            .ToList();

        Assert.Equal(1, results.Count);
        Assert.Equal("Bravo", results[0].Name);
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public string Name { get; set; }
    }
}
