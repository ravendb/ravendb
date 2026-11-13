using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Tests.Infrastructure;
using Xunit;
using PlanCache = Corax.Querying.Planning.PlanCache;

namespace FastTests.Corax;

/// <summary>
/// RavenDB-25281 collapse coverage for the post-filter clause families — spatial and vector — that
/// resolve their operands through a dedicated binding array rather than the ordinary scalar term path. The
/// structural key blanks WHERE literal VALUES (keeping type) and renumbers parameter NAMES, so a spatial
/// <c>spatial.circle(R, lat, lon, 'miles')</c> with different numeric R/lat/lon, or a parameterized
/// <c>vector.search(Embedding, $vec, $sim)</c> with different bound vectors/thresholds, must share ONE plan
/// bucket. The collapse is only safe because every spatial/vector operand is read through the per-query slot
/// vector at instantiation (ResolveSpatialFromBindings / ResolveVectorFromBindings → ResolveBindingScalar/Raw →
/// SlotBindingFor), never baked into the shared template. These tests pin both halves: the bucket count
/// collapses, and each collapsed query still returns its own geometry-/direction-correct result.
/// </summary>
public class PlanCacheSpatialVectorCollapseTests : RavenTestBase
{
    public PlanCacheSpatialVectorCollapseTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Geo
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
        public float[] Embedding { get; set; }
    }

    private class Geo_Index : AbstractIndexCreationTask<Geo>
    {
        public Geo_Index()
        {
            Map = docs => from d in docs
                select new
                {
                    d.Name,
                    Coordinates = CreateSpatialField(d.Lat, d.Lon),
                    Embedding = CreateVector(d.Embedding),
                };
        }
    }

    // east / north / north-east unit vectors. Cosine similarity is 1.0 against a same-direction query and
    // 0.0 (east vs north) or ~0.707 (east vs NE) otherwise, so a 0.9 threshold selects exactly the doc whose
    // embedding points the same way as the query vector — independent of the cosine→[0,1] scaling.
    private static readonly float[] East = [1f, 0f];
    private static readonly float[] North = [0f, 1f];
    private static readonly float[] NorthEast = [0.70710677f, 0.70710677f];

    // Three docs strung out east along the equator from the origin. At the equator one degree of longitude is
    // ~69 miles, so the longitudes below sit at ~0 / ~20.7 / ~62.2 miles from (0,0): a circle of 10 mi holds
    // only "Origin", 40 mi adds "Near", 100 mi adds "Far". Each doc also carries a distinct embedding direction.
    private static List<Geo> BuildSeed() =>
    [
        new Geo { Id = "geo/origin", Name = "Origin", Lat = 0, Lon = 0.0, Embedding = East },
        new Geo { Id = "geo/near", Name = "Near", Lat = 0, Lon = 0.3, Embedding = North },
        new Geo { Id = "geo/far", Name = "Far", Lat = 0, Lon = 0.9, Embedding = NorthEast },
    ];

    private async Task<(IDocumentStore Store, string IndexName, PlanCache Cache)> SetupAsync(Options options)
    {
        var store = GetDocumentStore(options);
        var index = new Geo_Index();
        index.Execute(store);
        using (var session = store.OpenSession())
        {
            foreach (var d in BuildSeed())
                session.Store(d, d.Id);
            session.SaveChanges();
        }
        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var serverIndex = database.IndexStore.GetIndex(index.IndexName);
        var cache = ((CoraxIndexPersistence)serverIndex.IndexPersistence).SharedPlanCache;
        return (store, index.IndexName, cache);
    }

    private static async Task<List<string>> NamesAsync(IAsyncDocumentSession session, string rql, params (string Name, object Value)[] parameters)
    {
        var query = session.Advanced.AsyncRawQuery<Geo>(rql);
        foreach (var (name, value) in parameters)
            query.AddParameter(name, value);
        var results = await query.ToListAsync();
        return results.Select(r => r.Name).OrderBy(n => n).ToList();
    }

    // spatial.circle(R, lat, lon, 'miles') variants differ only in numeric literal/parameter values, so all the
    // literal variants collapse onto one bucket and all the parameter variants onto a second (distinct because a
    // parameter operand is a different source than a literal). Each variant still resolves its own circle through
    // the slot vector, so a wider radius selects strictly more docs.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SpatialCircleValueAndParameterVariantsShareBucket_AndResolveOwnRadius(Options options)
    {
        var (store, idx, cache) = await SetupAsync(options);
        using (store)
        {
            int Buckets() => cache.Snapshot().Count;
            using var session = store.OpenAsyncSession();

            string Spatial(string circle) => $"from index '{idx}' where spatial.within(Coordinates, {circle})";

            // Literal radius variants — one shared bucket; each radius selects its own correct set.
            int before = Buckets();
            Assert.Equal(new[] { "Origin" },
                await NamesAsync(session, Spatial("spatial.circle(10, 0, 0, 'miles')")));
            Assert.Equal(new[] { "Near", "Origin" },
                await NamesAsync(session, Spatial("spatial.circle(40, 0, 0, 'miles')")));
            Assert.Equal(new[] { "Far", "Near", "Origin" },
                await NamesAsync(session, Spatial("spatial.circle(100, 0, 0, 'miles')")));
            Assert.Equal(before + 1, Buckets());

            // Parameter radius variants — a second shared bucket (parameter source ≠ literal source).
            before = Buckets();
            string ParamCircle = "spatial.circle($r, $lat, $lon, 'miles')";
            Assert.Equal(new[] { "Origin" },
                await NamesAsync(session, Spatial(ParamCircle), ("r", 10.0), ("lat", 0.0), ("lon", 0.0)));
            Assert.Equal(new[] { "Near", "Origin" },
                await NamesAsync(session, Spatial(ParamCircle), ("r", 40.0), ("lat", 0.0), ("lon", 0.0)));
            Assert.Equal(new[] { "Far", "Near", "Origin" },
                await NamesAsync(session, Spatial(ParamCircle), ("r", 100.0), ("lat", 0.0), ("lon", 0.0)));
            Assert.Equal(before + 1, Buckets());
        }
    }

    // vector.search(Embedding, $vec, $sim) variants differ only in their bound query vector and threshold, so all
    // three collapse onto one bucket. The query vector lives in the slot vector, so each variant matches the doc
    // pointing in its own direction — proving the shared template never bakes the first query's vector.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task VectorSearchParameterVariantsShareBucket_AndResolveOwnVector(Options options)
    {
        var (store, idx, cache) = await SetupAsync(options);
        using (store)
        {
            int Buckets() => cache.Snapshot().Count;
            using var session = store.OpenAsyncSession();

            string Rql = $"from index '{idx}' where vector.search(Embedding, $vec, $sim)";

            int before = Buckets();
            Assert.Equal(new[] { "Origin" },
                await NamesAsync(session, Rql, ("vec", East), ("sim", 0.9f)));
            Assert.Equal(new[] { "Near" },
                await NamesAsync(session, Rql, ("vec", North), ("sim", 0.9f)));
            Assert.Equal(new[] { "Far" },
                await NamesAsync(session, Rql, ("vec", NorthEast), ("sim", 0.9f)));
            Assert.Equal(before + 1, Buckets());
        }
    }
}
