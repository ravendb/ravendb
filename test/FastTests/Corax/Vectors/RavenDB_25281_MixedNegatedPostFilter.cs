using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

/// <summary>
/// A query with BOTH a negated spatial and a negated vector clause exercises multi-clause subtraction through the
/// single NegatedPostFilterMatch: R \ (spatial matches) \ (vector matches). The term clause (City = 'NYC') is the
/// candidate universe; each negated clause is scoped to it and subtracted in turn.
/// </summary>
public class RavenDB_25281_MixedNegatedPostFilter(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class GeoVecDoc
    {
        public string Id { get; set; }
        public string City { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
        public float[] Embedding { get; set; }
    }

    private sealed class GeoVecIndex : AbstractIndexCreationTask<GeoVecDoc>
    {
        public GeoVecIndex()
        {
            Map = docs => from d in docs
                          select new
                          {
                              d.City,
                              Location = CreateSpatialField(d.Lat, d.Lon),
                              Embedding = CreateVector(d.Embedding),
                          };
        }
    }

    private static readonly float[] Query = [1f, 0f];

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Negated_spatial_and_negated_vector_subtract_through_one_wrapper()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            // All are City = NYC (the candidate universe R = {1,2,3,4}). Embeddings are distinct (no ties) so
            // vector.search's numberOfCandidates selects a deterministic set of documents: in Corax's HNSW
            // storage, identical embeddings collapse onto the same graph node and expand via that node's
            // posting list, so numberOfCandidates bounds distinct-vector nodes rather than raw document count.
            // Distinct embeddings avoid that expansion and keep "top N nearest" == "N documents".
            session.Store(new GeoVecDoc { Id = "docs/1", City = "NYC", Lat = 0, Lon = 0, Embedding = [1.0f, 0.0f] });   // in circle, sim 1.0 (nearest)
            session.Store(new GeoVecDoc { Id = "docs/2", City = "NYC", Lat = 0, Lon = 0, Embedding = [0.0f, 1.0f] });   // in circle, sim 0.0
            session.Store(new GeoVecDoc { Id = "docs/3", City = "NYC", Lat = 30, Lon = 30, Embedding = [0.8f, 0.6f] }); // out of circle, sim 0.8 (nearer of the two remaining after spatial)
            session.Store(new GeoVecDoc { Id = "docs/4", City = "NYC", Lat = 30, Lon = 30, Embedding = [0.6f, 0.8f] }); // out of circle, sim 0.6
            session.SaveChanges();
        }

        new GeoVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();
        var ids = session2.Advanced
            .RawQuery<GeoVecDoc>(
                "from index 'GeoVecIndex' where City = $c " +
                "and not spatial.within(Location, spatial.circle(60, 0, 0, 'miles')) " +
                "and not vector.search(Embedding, $vec, 0.0, 1)")
            .AddParameter("c", "NYC")
            .AddParameter("vec", Query)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .OrderBy(id => id)
            .ToList();

        // R = {1,2,3,4}.
        // not within(origin circle) removes the origin docs {1,2} -> R' = {3,4}.
        // Each negated clause is scoped to the current R (not the original universe), so vector.search's
        // top-1-nearest is computed over R' = {3,4}, not over all 4 docs: it picks docs/3 (sim 0.8 > 0.6).
        // not vector.search([1,0], top 1) removes docs/3 from R' -> {4}.
        Assert.Equal(new[] { "docs/4" }, ids);
    }
}
