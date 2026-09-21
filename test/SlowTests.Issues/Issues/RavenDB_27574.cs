using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27574 : RavenTestBase
{
    public RavenDB_27574(ITestOutputHelper output) : base(output)
    {
    }

    private const int Matching = 64;

    private const string Nested = "where Year = 2000 and (Status = 'Nope' or Tags all in ($p))";

    // An empty IN is collapsed to "match nothing" before slot resolution, but only for top-level clauses, so a nested
    // one keeps its slot run. With no terms the synthetic null slot is the clause's only slot and takes the Fill op,
    // where ALL IN's "everything" identity is rejected outright.
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ANestedEmptyAllInMatchesNothing(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();

        Assert.Equal(0, Total(session, Nested, new string[0]));

        // the boosted dispatch resolves this slot in the arm above, and answers 0 here for its own reasons - every
        // scored ALL IN does, see RavenDB-27573 - so this pins the shape, not the semantics
        Assert.Equal(0, Total(session, Nested + " order by score()", new string[0]));

        // controls: plain IN carries the other identity, and a top-level clause is collapsed before it gets here
        Assert.Equal(0, Total(session, Nested.Replace("all in", "in"), new string[0]));
        Assert.Equal(0, Total(session, "where Tags all in ($p)", new string[0]));

        // control: the same nested shape with a real term
        Assert.Equal(Matching, Total(session, Nested, new[] { "java" }));
    }

    // the slot run is per clause, so the field's type does not change the outcome
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ANestedEmptyAllInMatchesNothingOnANumericField(Options options)
    {
        using var store = GetDocumentStore(options);
        Seed(store);

        using var session = store.OpenSession();

        const string nested = "where Status = 'Released' and (Status = 'Nope' or Year all in ($p))";

        Assert.Equal(0, Total(session, nested, new long[0]));
        Assert.Equal(Matching, Total(session, nested, new long[] { 2000 }));
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

    private static long Total(IDocumentSession session, string where, object p)
    {
        session.Advanced
            .RawQuery<Movie>($"from index 'Movies/Showcase' {where} limit 1")
            .AddParameter("p", p)
            .Statistics(out var stats)
            .ToList();

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
