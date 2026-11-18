using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Regression coverage for `not spatial.within(...)` in Corax 2.0. Spatial clauses are lifted out of the
/// bitmap pipeline by GroupCollapse and applied as a PostFilterMatch, which previously only intersected
/// (AndWith) the candidate set — dropping the clause's IsNegated flag, so a negated spatial predicate
/// returned the documents INSIDE the shape instead of outside it. The fix keeps the post-filter optimization
/// (the predicate still runs only against the candidate set) but subtracts the matches for a negated clause.
/// </summary>
public class RavenDB_25281_NegatedSpatialPostFilter : RavenTestBase
{
    public RavenDB_25281_NegatedSpatialPostFilter(ITestOutputHelper output) : base(output)
    {
    }

    // 60-mile circle around the origin: contains the origin docs, excludes the ~2900-mile-away docs.
    private const string OriginCircle = "spatial.circle(60, 0, 0, 'miles')";
    // 6000-mile circle around the origin: contains every doc in the set (the (30,30) docs are ~2900 miles out).
    private const string WorldCircle = "spatial.circle(6000, 0, 0, 'miles')";

    private static IDocumentStore PopulateStore(RavenTestBase test)
    {
        var docs = new List<Place>
        {
            new() { Name = "Inside-Keep",   Tag = "keep",  Lat = 0,  Lon = 0  },  // inside circle
            new() { Name = "Outside-Keep",  Tag = "keep",  Lat = 30, Lon = 30 },  // outside circle
            new() { Name = "Inside-Other",  Tag = "other", Lat = 0,  Lon = 0  },  // inside circle
            new() { Name = "Outside-Other", Tag = "other", Lat = 30, Lon = 30 },  // outside circle
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
    public void Positive_spatial_within_is_the_baseline()
    {
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where spatial.within(Location, {OriginCircle})")
            .WaitForNonStaleResults()
            .ToList();

        // Sanity: positive predicate keeps the docs inside the circle.
        Assert.Equal(new[] { "Inside-Keep", "Inside-Other" }, Names(results));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Negated_spatial_within_over_term_candidates_returns_the_complement_within_candidates()
    {
        // The `Tag = 'keep'` bitmap clause is the driver (candidate set = {Inside-Keep, Outside-Keep}); the
        // spatial predicate runs only against those two candidates. Negation must exclude the one inside the
        // circle, leaving only Outside-Keep. Under the bug (IsNegated dropped) this returned Inside-Keep.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where Tag = 'keep' and not spatial.within(Location, {OriginCircle})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(new[] { "Outside-Keep" }, Names(results));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Pure_spatial_positive_driver_with_negated_spatial_rest_returns_the_complement()
    {
        // Both clauses are spatial, so there is no bitmap-pipeline driver (source is null). The positive
        // `within(WorldCircle)` (all docs) drives; the negated `within(OriginCircle)` is subtracted → far docs.
        // Exercises the source==null branch: positive spatial as driver, negated spatial as a post-filter.
        // Under the bug the negation was dropped, intersecting instead → the two origin docs.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where spatial.within(Location, {WorldCircle}) and not spatial.within(Location, {OriginCircle})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(new[] { "Outside-Keep", "Outside-Other" }, Names(results));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Pure_negated_spatial_with_no_positive_clause_falls_back_to_all_entries_universe()
    {
        // RQL has no leading unary NOT (negation only exists as "AND NOT" / "OR NOT"), so "true and not ..." is
        // the only way to express a query with no positive clause besides the negated spatial predicate. The
        // leading `true` parses to BooleanOp.True, not a bitmap-pipeline clause, so exec.IsAllEntries stays true
        // and ApplyPostFilters is invoked with source==null: there is no bitmap-pipeline driver to serve as the
        // candidate universe, so the routing falls back to builderParameters.IndexSearcher.AllEntries() before
        // wrapping in NegatedPostFilterMatch. This exercises that AllEntries() universe fallback specifically.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where true and not spatial.within(Location, {OriginCircle})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(new[] { "Outside-Keep", "Outside-Other" }, Names(results));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Negated_spatial_over_large_candidate_set_uses_shape_driven_filtering()
    {
        // With a candidate universe larger than SpatialMatch's CandidateScanLimit (2048), the negated-spatial
        // filter switches from candidate-driven (iterate R, geo-test each) to shape-driven (enumerate the shape's
        // geohash cells, use R only as a membership pre-filter). This seeds > 2048 docs so the pure-negated query
        // (universe = AllEntries) takes the shape-driven branch, and asserts it still returns exactly the docs
        // outside the circle.
        const int insideCount = 200;
        const int outsideCount = 2100; // total 2300 > 2048 → shape-driven

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < insideCount; i++)
                session.Store(new Place { Name = $"in-{i}", Tag = "in", Lat = 0, Lon = 0 });
            for (int i = 0; i < outsideCount; i++)
                session.Store(new Place { Name = $"out-{i}", Tag = "out", Lat = 60, Lon = 60 });
            session.SaveChanges();
        }
        new PlacesIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var outsideMatches = session.Advanced.RawQuery<Place>(
                    $"from index 'PlacesIndex' where true and not spatial.within(Location, {OriginCircle})")
                .WaitForNonStaleResults()
                .ToList();
            Assert.Equal(outsideCount, outsideMatches.Count);
            Assert.True(outsideMatches.All(p => p.Tag == "out"));

            // Sanity: the positive predicate over the same large set keeps exactly the inside docs.
            var insideMatches = session.Advanced.RawQuery<Place>(
                    $"from index 'PlacesIndex' where spatial.within(Location, {OriginCircle})")
                .WaitForNonStaleResults()
                .ToList();
            Assert.Equal(insideCount, insideMatches.Count);
            Assert.True(insideMatches.All(p => p.Tag == "in"));
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
