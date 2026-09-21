using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27573 : RavenTestBase
{
    public RavenDB_27573(ITestOutputHelper output) : base(output)
    {
    }

    private const int Matching = 64;

    // An In/AllIn clause allocates a synthetic null-term slot that has to carry the merge operator's identity:
    // nothing for IN (OR), everything for ALL IN (AND). `order by score()` switches the clause to the query-match
    // dispatch, whose arm used the OR identity for both, so the ALL IN intersection was ANDed against an empty match.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AllInKeepsItsResultsWhenOrderedByScore(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();

        Assert.Equal(Matching, Total(session, "where Status all in ('Released')"));
        // control: the same single term through IN, where the empty identity happens to be the right one
        Assert.Equal(Matching, Total(session, "where Status in ('Released') order by score()"));
        // control: ordering as such is not the trigger, only score ordering is
        Assert.Equal(Matching, Total(session, "where Status all in ('Released') order by Year"));

        Assert.Equal(Matching, Total(session, "where Status all in ('Released') order by score()"));
    }

    // the slot is allocated per clause, so the field's type and arity do not change the outcome
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AllInKeepsItsResultsOnArrayAndNumericFields(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();

        Assert.Equal(Matching, Total(session, "where Tags all in ('java') order by score()"));
        Assert.Equal(Matching, Total(session, "where Tags all in ('java', 'sql') order by score()"));
        Assert.Equal(Matching, Total(session, "where Year all in (2000) order by score()"));
    }

    // An empty list leaves the null slot as the clause's ONLY slot, which puts the Fill op on it: the slot is then
    // the seed rather than an operand, and an AND identity there would seed the whole index. A top-level empty IN is
    // collapsed to "match nothing" before it reaches slot resolution, a nested one is not.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ANestedEmptyAllInStillMatchesNothing(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();

        const string nested = "where Year = 2000 and (Status = 'Nope' or Tags all in ($p))";

        Assert.Equal(0, Total(session, nested + " order by score()", new string[0]));
        Assert.Equal(0, Total(session, nested.Replace("all in", "in") + " order by score()", new string[0]));
        // control: the same shape with a real term is the defect this fix is for
        Assert.Equal(Matching, Total(session, nested + " order by score()", new[] { "java" }));
    }

    // negation builds the intersection into a scratch slot and subtracts it, so an empty identity there did not just
    // lose results - it dropped the NOT entirely and the query over-matched
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NegatedAllInKeepsItsComplementWhenOrderedByScore(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();

        Assert.Equal(Matching, Total(session, "where Year = 2000 and not (Tags all in ('c#', 'rust')) order by score()"));
        Assert.Equal(Matching, Total(session, "where Status = 'Planned' or not (Tags all in ('java', 'sql')) order by score()"));
    }

    private void Seed(IDocumentStore store)
    {
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < Matching * 2; i++)
            {
                var released = i % 2 == 0;
                session.Store(new Movie
                {
                    Status = released ? "Released" : "Planned",
                    Year = released ? 2000 : 2001,
                    Tags = released ? new[] { "java", "sql" } : new[] { "c#", "rust" }
                });
            }

            session.SaveChanges();
        }

        new Movies_Showcase().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    // mirrors the report: a one-document page, and the assertion is on the total rather than on the page
    private static long Total(IDocumentSession session, string where, string[] p = null)
    {
        var query = session.Advanced.RawQuery<Movie>($"from index 'Movies/Showcase' {where} limit 1");
        if (p != null)
            query = query.AddParameter("p", p);

        query.Statistics(out var stats).ToList();

        return stats.TotalResults;
    }

    private class Movie
    {
        public string Id { get; set; }

        public string Status { get; set; }

        public string[] Tags { get; set; }

        public int Year { get; set; }
    }

    private class Movies_Showcase : AbstractIndexCreationTask<Movie>
    {
        public Movies_Showcase()
        {
            Map = movies => from m in movies
                            select new { m.Status, m.Tags, m.Year };
        }
    }
}
