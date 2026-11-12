using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class ConjunctionScoringUnderOr(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(RavenSearchEngineMode.Lucene, "Tag in ('x', 'y') and startsWith(Name, 'mar')")]
    [InlineData(RavenSearchEngineMode.Corax, "Tag in ('x', 'y') and startsWith(Name, 'mar')")]
    [InlineData(RavenSearchEngineMode.Lucene, "Tag in ('x', 'y') and exists(Optional)")]
    [InlineData(RavenSearchEngineMode.Corax, "Tag in ('x', 'y') and exists(Optional)")]
    [InlineData(RavenSearchEngineMode.Lucene, "Name = 'maciej' and startsWith(Name, 'mar')")]
    [InlineData(RavenSearchEngineMode.Corax, "Name = 'maciej' and startsWith(Name, 'mar')")]
    [InlineData(RavenSearchEngineMode.Lucene, "startsWith(Name, 'mar') and not Optional = 'x'")]
    [InlineData(RavenSearchEngineMode.Corax, "startsWith(Name, 'mar') and not Optional = 'x'")]
    [InlineData(RavenSearchEngineMode.Lucene, "Optional = 'y' and startsWith(Name, 'mar')")]
    [InlineData(RavenSearchEngineMode.Corax, "Optional = 'y' and startsWith(Name, 'mar')")]
    public void ConjunctionUnderOrScoresOnlyItsOwnMatches(RavenSearchEngineMode engine, string conjunction)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(engine, includeScoresAndDistances: true));
        using (var commands = store.Commands())
        {
            var metadata = new Dictionary<string, object> { [Raven.Client.Constants.Documents.Metadata.Collection] = "Items" };
            commands.Put("items/1", null, new { Tag = "a", Name = "Maciej" }, metadata);
            commands.Put("items/2", null, new { Tag = "a", Name = "Marika", Optional = "x" }, metadata);
            commands.Put("items/3", null, new { Tag = "b", Name = "Marika", Optional = "y" }, metadata);
        }
        
        var withConjunction = Scores(store, $"from Items where ({conjunction}) or Tag = 'a' order by score()");
        var withoutConjunction = Scores(store, "from Items where Tag = 'a' order by score()");

        Assert.Equal(withoutConjunction["items/2"] / withoutConjunction["items/1"], withConjunction["items/2"] / withConjunction["items/1"], 4);
    }

    private static Dictionary<string, double> Scores(IDocumentStore store, string rql)
    {
        using var session = store.OpenSession();
        return session.Advanced.RawQuery<Item>(rql).WaitForNonStaleResults().ToList()
            .ToDictionary(x => x.Id, x => Convert.ToDouble(session.Advanced.GetMetadataFor(x)[Raven.Client.Constants.Documents.Metadata.IndexScore]));
    }

    private class Item
    {
        public string Id { get; set; }
    }
}
