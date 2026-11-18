using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Regression coverage for `when($flag, spatial.within(...))` / `when($flag, vector.search(...))` in Corax 2.0.
/// GroupCollapse lifts spatial/vector clauses out of the top-level Clauses list into
/// PlanTemplate.SpatialClauses/VectorClauses before WhenRegister runs; ApplyFate (which collapses a false-WHEN
/// clause to a MatchAll sentinel) only ever walks template.Clauses, so it structurally cannot see them.
/// AttachSpatialAndVectorClauses (which builds the actual spatial/vector matchers) never inspects WhenCondition
/// either. The net effect: a spatial/vector clause wrapped in when($flag, ...) is ALWAYS applied, regardless of
/// the flag's value — when($flag=false, ...) silently keeps filtering instead of collapsing to a no-op under AND.
/// </summary>
public class RavenDB_25281_WhenSpatialVector : RavenTestBase
{
    public RavenDB_25281_WhenSpatialVector(ITestOutputHelper output) : base(output)
    {
    }

    // 60-mile circle around the origin: contains the origin docs, excludes the ~2900-mile-away docs.
    private const string OriginCircle = "spatial.circle(60, 0, 0, 'miles')";

    private static IDocumentStore PopulateStore(RavenTestBase test)
    {
        var docs = new List<Place>
        {
            new() { Name = "John", Tag = "keep", Lat = 0, Lon = 0 }, // inside circle
            new() { Name = "John", Tag = "keep", Lat = 30, Lon = 30 }, // outside circle
            new() { Name = "Jane", Tag = "keep", Lat = 0, Lon = 0 }, // inside circle, wrong name
        };

        var store = test.GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            foreach (var d in docs)
                session.Store(d);
            session.SaveChanges();
        }
        new PlacesIndex().Execute(store);
        test.Indexes.WaitForIndexing(store);
        return store;
    }

    private static string[] Names(IEnumerable<Place> results) => results.Select(r => r.Name).OrderBy(n => n).ToArray();

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void WhenFalse_spatial_clause_collapses_to_match_all_under_and()
    {
        using var store = PopulateStore(this);
        using var session = store.OpenSession();

        // flag=false -> the spatial clause must behave as a no-op (match-all under AND): both John docs
        // (inside and outside the circle) must be returned, location-independent.
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where when($flag = true, spatial.within(Location, {OriginCircle})) and Tag = $tag and Name = $name")
            .AddParameter("flag", false)
            .AddParameter("tag", "keep")
            .AddParameter("name", "John")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("John", r.Name));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void WhenTrue_spatial_clause_is_applied_as_normal()
    {
        using var store = PopulateStore(this);
        using var session = store.OpenSession();

        // flag=true -> the spatial guard is on, so the circle filter applies: only the inside-circle John doc.
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where when($flag = true, spatial.within(Location, {OriginCircle})) and Tag = $tag and Name = $name")
            .AddParameter("flag", true)
            .AddParameter("tag", "keep")
            .AddParameter("name", "John")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(1, results.Count);
        Assert.Equal("John", results[0].Name);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void WhenFalse_vector_clause_collapses_to_match_all_under_and()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        SeedVectorDocs(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        // flag=false -> the vector search must behave as a no-op (match-all under AND): every NYC doc is
        // returned, regardless of similarity to the query vector.
        var ids = session.Advanced
            .RawQuery<CityVecDoc>(
                "from index 'CityVecIndex' where City = $c and when($flag = true, vector.search(Embedding, $vec, 0.0, 2))")
            .AddParameter("flag", false)
            .AddParameter("c", "NYC")
            .AddParameter("vec", VectorQuery)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .OrderBy(id => id)
            .ToList();

        Assert.Equal(new[] { "docs/nyc-1", "docs/nyc-2", "docs/nyc-3" }, ids);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void WhenTrue_vector_clause_is_applied_as_normal()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        SeedVectorDocs(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        // flag=true -> the vector guard is on, so only the top-2 nearest NYC docs are returned.
        var ids = session.Advanced
            .RawQuery<CityVecDoc>(
                "from index 'CityVecIndex' where City = $c and when($flag = true, vector.search(Embedding, $vec, 0.0, 2))")
            .AddParameter("flag", true)
            .AddParameter("c", "NYC")
            .AddParameter("vec", VectorQuery)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .OrderBy(id => id)
            .ToList();

        Assert.Equal(new[] { "docs/nyc-1", "docs/nyc-2" }, ids);
    }

    // Query vector points east ([1,0]); the NYC docs sit on a decreasing cosine-similarity gradient against it.
    private static readonly float[] VectorQuery = [1f, 0f];

    private static void SeedVectorDocs(IDocumentStore store)
    {
        using var session = store.OpenSession();
        session.Store(new CityVecDoc { Id = "docs/nyc-1", City = "NYC", Embedding = [1.0f, 0.0f] }); // sim 1.0
        session.Store(new CityVecDoc { Id = "docs/nyc-2", City = "NYC", Embedding = [0.8f, 0.6f] }); // sim 0.8
        session.Store(new CityVecDoc { Id = "docs/nyc-3", City = "NYC", Embedding = [0.6f, 0.8f] }); // sim 0.6
        session.SaveChanges();
    }

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

    private class PlacesIndex : AbstractIndexCreationTask<Place>
    {
        public PlacesIndex()
        {
            Map = places => places.Select(p => new
            {
                p.Name,
                p.Tag,
                Location = CreateSpatialField(p.Lat, p.Lon),
            });
        }
    }

    private class Place
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
    }
}
