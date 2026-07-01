using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class RavenDB_25281_NestedOrSpatialVector(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class GeoVecDoc
    {
        public string Id { get; set; }
        public string Tag { get; set; }
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
                              d.Tag,
                              Coordinates = CreateSpatialField(d.Lat, d.Lon),
                              Embedding = CreateVector(d.Embedding),
                          };
        }
    }

    // east unit vector — cosine similarity 1.0 against a [1,0] query, 0.0 against [0,1].
    private static readonly float[] East = [1f, 0f];

    // north unit vector — cosine similarity 0.0 against a [1,0] query.
    private static readonly float[] North = [0f, 1f];

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void NestedOrWithSpatialAndVectorLeavesReturnsUnionOfBothBranches()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        // Branch A := (Tag == "A" AND vector.search(Embedding, [1,0], 0.9))  -> tag A AND an east-pointing embedding
        // Branch B := (Tag == "B" AND spatial.within(Coordinates, circle(60mi @ (0,0)))) -> tag B AND inside the circle
        // The query is (Branch A) OR (Branch B). Inside an OR branch the spatial / vector clauses are ordinary
        // pipeline leaves (the planner only lifts them to top-level post-filters in an AND context), so this shape
        // exercises the nested-OR path that the IsPostFilter-by-type bug would have mishandled.
        using (var session = store.OpenSession())
        {
            // Matches branch A: tag A, east embedding (far from the circle — irrelevant for branch A).
            session.Store(new GeoVecDoc { Id = "docs/1", Tag = "A", Lat = 40, Lon = 40, Embedding = East });
            // tag A but north embedding -> fails branch A's vector filter; tag != B -> not branch B.
            session.Store(new GeoVecDoc { Id = "docs/2", Tag = "A", Lat = 40, Lon = 40, Embedding = North });
            // Matches branch B: tag B, inside the circle (embedding direction irrelevant for branch B).
            session.Store(new GeoVecDoc { Id = "docs/3", Tag = "B", Lat = 0, Lon = 0, Embedding = North });
            // tag B but outside the circle -> fails branch B's spatial filter; tag != A -> not branch A.
            session.Store(new GeoVecDoc { Id = "docs/4", Tag = "B", Lat = 40, Lon = 40, Embedding = East });
            // tag A, inside the circle, north embedding -> branch A fails (vector), branch B fails (tag) -> neither.
            session.Store(new GeoVecDoc { Id = "docs/5", Tag = "A", Lat = 0, Lon = 0, Embedding = North });
            // Matches branch B: tag B, inside the circle, east embedding (the east embedding does not pull it into
            // branch A because that branch also requires tag A).
            session.Store(new GeoVecDoc { Id = "docs/6", Tag = "B", Lat = 0, Lon = 0, Embedding = East });
            // Matches branch A: tag A, east embedding, also inside the circle (still only one branch — tag is A).
            session.Store(new GeoVecDoc { Id = "docs/7", Tag = "A", Lat = 0, Lon = 0, Embedding = East });
            // Unrelated tag -> neither branch regardless of geometry / embedding.
            session.Store(new GeoVecDoc { Id = "docs/8", Tag = "C", Lat = 0, Lon = 0, Embedding = East });
            session.SaveChanges();
        }

        new GeoVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var ids = session.Advanced
                .RawQuery<GeoVecDoc>(
                    @"from index 'GeoVecIndex'
                      where (Tag = $tagA and vector.search(Embedding, $vec, $minSim))
                         or (Tag = $tagB and spatial.within(Coordinates, spatial.circle($r, $lat, $lon, 'miles')))")
                .AddParameter("tagA", "A")
                .AddParameter("vec", East)
                .AddParameter("minSim", 0.9f)
                .AddParameter("tagB", "B")
                .AddParameter("r", 60.0)
                .AddParameter("lat", 0.0)
                .AddParameter("lon", 0.0)
                .WaitForNonStaleResults()
                .ToList()
                .Select(d => d.Id)
                .OrderBy(id => id)
                .ToArray();

            Assert.Equal(new[] { "docs/1", "docs/3", "docs/6", "docs/7" }, ids);
        }
    }
}
