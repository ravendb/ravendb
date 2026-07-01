using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

// RavenDB-25281: a search(field, "a b c") clause compiles to a SINGLE bitmap-pipeline leaf, but it executes as a
// match over multiple analyzer-tokenized terms (OR/AND, or a phrase for a quoted group). These tests pin that the
// query plan surfaces that multi-term reality — the tokenized terms, their count, and the operator — instead of
// only the raw literal search string.
public class RavenDB_25281_SearchPlanInspectionTests : RavenTestBase
{
    public RavenDB_25281_SearchPlanInspectionTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Body { get; set; }
    }

    private class Docs_BySearchBody : AbstractIndexCreationTask<Doc>
    {
        public Docs_BySearchBody()
        {
            Map = docs => from d in docs
                select new { d.Body };
            Index(x => x.Body, FieldIndexing.Search);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SearchClause_PlanSurfacesTokenizedTermsAndOperator(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Docs_BySearchBody();
        index.Execute(store);

        using (var s = store.OpenAsyncSession())
        {
            await s.StoreAsync(new Doc { Body = "the love boat is sailing" });
            await s.StoreAsync(new Doc { Body = "love is in the air" });
            await s.StoreAsync(new Doc { Body = "a boat on the river" });
            await s.StoreAsync(new Doc { Body = "nothing relevant here" });
            await s.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // Three whitespace-separated words, default operator -> a 3-term OR match.
        var or = await SearchPlanParams(session, index.IndexName, "where search(Body, 'the love boat')");
        Assert.Equal("the, love, boat", or["SearchTerms"]);
        Assert.Equal("3", or["SearchTermCount"]);
        Assert.Equal("Or", or["SearchOperator"]);

        // Explicit AND operator is surfaced.
        var and = await SearchPlanParams(session, index.IndexName, "where search(Body, 'love boat', and)");
        Assert.Equal("love, boat", and["SearchTerms"]);
        Assert.Equal("2", and["SearchTermCount"]);
        Assert.Equal("And", and["SearchOperator"]);

        // A quoted group is a single phrase term, not three terms.
        var phrase = await SearchPlanParams(session, index.IndexName, "where search(Body, '\"the love boat\"')");
        Assert.Equal("the love boat", phrase["SearchTerms"]);
        Assert.Equal("1", phrase["SearchTermCount"]);
    }

    private static async Task<Dictionary<string, string>> SearchPlanParams(IAsyncDocumentSession session, string indexName, string whereClause)
    {
        await session.Advanced
            .AsyncRawQuery<Doc>($"from index '{indexName}' {whereClause} include timings()")
            .Timings(out var timings)
            .ToListAsync();

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        var search = FindNodeWithParam(plan, "SearchTerms");
        Assert.True(search != null, "Expected a plan node carrying SearchTerms (a search() leaf). Plan: " + Describe(plan));
        return search.Parameters;
    }

    private static QueryInspectionNode FindNodeWithParam(QueryInspectionNode node, string paramKey)
    {
        if (node == null)
            return null;
        if (node.Parameters != null && node.Parameters.ContainsKey(paramKey))
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindNodeWithParam(child, paramKey);
            if (found != null)
                return found;
        }

        return null;
    }

    private static string Describe(QueryInspectionNode node, int depth = 0)
    {
        if (node == null)
            return "<null>";
        var line = new string(' ', depth * 2) + node.Operation;
        if (node.Children == null || node.Children.Count == 0)
            return line;
        return line + "\n" + string.Join("\n", node.Children.Select(c => Describe(c, depth + 1)));
    }
}
