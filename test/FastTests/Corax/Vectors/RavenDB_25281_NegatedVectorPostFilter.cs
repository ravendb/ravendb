using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

/// <summary>
/// Regression coverage for `not vector.search(...)` in Corax 2.0. A vector clause is lifted out of the bitmap
/// pipeline and applied as a post-filter that intersects the candidate set with the approximate-NN matches.
/// That path dropped the clause's IsNegated flag, so a negated vector predicate kept the docs nearest the query
/// vector instead of excluding them. The fix keeps the candidate-set optimization (the search still runs only
/// against the candidates) but streams candidates \ matches for a negated clause.
/// </summary>
public class RavenDB_25281_NegatedVectorPostFilter(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class CityVecDoc
    {
        public string Id { get; set; }
        public string City { get; set; }
        public float[] Embedding { get; set; }
    }

    private sealed class CityVecIndex : AbstractIndexCreationTask<CityVecDoc>
    {
        public CityVecIndex()
        {
            Map = docs => from d in docs
                          select new
                          {
                              d.City,
                              Embedding = CreateVector(d.Embedding),
                          };
        }
    }

    // Query vector points east ([1,0]); the NYC docs sit on a decreasing cosine-similarity gradient against it.
    private static readonly float[] Query = [1f, 0f];

    private static void Seed(IDocumentStore store)
    {
        using var session = store.OpenSession();
        session.Store(new CityVecDoc { Id = "docs/nyc-1", City = "NYC", Embedding = [1.0f, 0.0f] }); // sim 1.0
        session.Store(new CityVecDoc { Id = "docs/nyc-2", City = "NYC", Embedding = [0.8f, 0.6f] }); // sim 0.8
        session.Store(new CityVecDoc { Id = "docs/nyc-3", City = "NYC", Embedding = [0.6f, 0.8f] }); // sim 0.6
        // Most-similar doc of all, but a different city — proves the negated vector still post-filters only the
        // candidate set (NYC), never the whole index.
        session.Store(new CityVecDoc { Id = "docs/par-1", City = "Paris", Embedding = [1.0f, 0.0f] }); // sim 1.0
        session.SaveChanges();
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Positive_vector_search_over_term_candidates_is_the_baseline()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var ids = session.Advanced
            .RawQuery<CityVecDoc>("from index 'CityVecIndex' where City = $c and vector.search(Embedding, $vec, 0.0, 2)")
            .AddParameter("c", "NYC")
            .AddParameter("vec", Query)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .OrderBy(id => id)
            .ToList();

        // The 2 nearest NYC docs to [1,0] are nyc-1 (1.0) and nyc-2 (0.8); nyc-3 (0.6) is the 3rd.
        Assert.Equal(new[] { "docs/nyc-1", "docs/nyc-2" }, ids);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Negated_vector_search_returns_candidates_the_search_does_not_match()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var ids = session.Advanced
            .RawQuery<CityVecDoc>("from index 'CityVecIndex' where City = $c and not vector.search(Embedding, $vec, 0.0, 2)")
            .AddParameter("c", "NYC")
            .AddParameter("vec", Query)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .OrderBy(id => id)
            .ToList();

        // NYC \ {nyc-1, nyc-2} = {nyc-3}. Under the bug (negation dropped) this returned {nyc-1, nyc-2}.
        Assert.Equal(new[] { "docs/nyc-3" }, ids);
    }
}
