using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Multi-spatial predicate coverage for Corax. The three shapes exercise three distinct
/// resolution paths in <c>QueryPlanBuilder.Resolution</c>:
///   1. Pure-spatial AND  → InstantiateAllEntriesPostFilter (PostFilterMatch chain over spatial[0])
///   2. Pure-spatial OR   → standard bitmap pipeline (spatial clauses stay in main list, GroupCollapse skips for OR)
///   3. Spatial AND term  → standard bitmap pipeline (CompiledQueryMatch over term, then PostFilterMatch with spatial)
/// </summary>
public class SpatialMultiPredicateCorax : RavenTestBase
{
    public SpatialMultiPredicateCorax(ITestOutputHelper output) : base(output)
    {
    }

    private const string CircleOriginMiles = "spatial.circle(60, 0, 0, 'miles')";
    private const string CircleFarMiles = "spatial.circle(60, 0, 0, 'miles')";

    private static IDocumentStore PopulateStore(RavenTestBase test)
    {
        // Four documents at the corners of {near-origin × near-origin}, indexed against
        // two independent spatial fields. Each query below selects a specific subset.
        var docs = new List<Place>
        {
            new() { Name = "Alpha",   Lat1 = 0,    Lon1 = 0,    Lat2 = 0,    Lon2 = 0    }, // in both circles
            new() { Name = "Bravo",   Lat1 = 0,    Lon1 = 0,    Lat2 = 30,   Lon2 = 30   }, // in circle1 only
            new() { Name = "Charlie", Lat1 = 30,   Lon1 = 30,   Lat2 = 0,    Lon2 = 0    }, // in circle2 only
            new() { Name = "Delta",   Lat1 = 30,   Lon1 = 30,   Lat2 = 30,   Lon2 = 30   }, // in neither
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

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Spatial_AND_Spatial_returns_intersection_of_both_circles()
    {
        // Hits InstantiateAllEntriesPostFilter: PostFilterMatch(spatial[0], [spatial[1]]).
        // GroupCollapse partitions both spatial clauses out of the main list → IsAllEntries=true,
        // SpatialFilters.Length == 2 → bypass fires, no CompiledQueryMatch is constructed.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where spatial.within(Location1, {CircleOriginMiles}) and spatial.within(Location2, {CircleFarMiles})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(1, results.Count);
        Assert.Equal("Alpha", results[0].Name);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Spatial_OR_Spatial_returns_union_of_both_circles()
    {
        // Standard bitmap pipeline. GroupCollapse early-returns for IsOr=true, so both
        // spatial clauses stay in the main clause list and are OR'd by CompiledQueryMatch.
        // exec.SpatialFilters is null here — the bypass does NOT fire.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where spatial.within(Location1, {CircleOriginMiles}) or spatial.within(Location2, {CircleFarMiles})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(3, results.Count);
        var names = results.Select(r => r.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "Alpha", "Bravo", "Charlie" }, names);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Spatial_AND_term_postfilters_spatial_against_compiled_term_match()
    {
        // Standard bitmap pipeline. Term predicate stays in main clause list → IsAllEntries=false.
        // Spatial is partitioned by GroupCollapse into exec.SpatialFilters → PostFilterMatch
        // wraps the CompiledQueryMatch(Name='Alpha') with the spatial filter.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where Name = 'Alpha' and spatial.within(Location1, {CircleOriginMiles})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(1, results.Count);
        Assert.Equal("Alpha", results[0].Name);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Spatial_AND_Spatial_with_no_matches_returns_empty()
    {
        // Edge case: bypass returns empty when the two spatial filters disagree on every doc.
        // Uses a far-away circle for Location1 so no doc is in both.
        using var store = PopulateStore(this);
        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                "from index 'PlacesIndex' " +
                "where spatial.within(Location1, spatial.circle(60, 50, 50, 'miles')) " +
                $"and spatial.within(Location2, {CircleFarMiles})")
            .WaitForNonStaleResults()
            .ToList();

        Assert.Empty(results);
    }

    private class PlacesIndex : AbstractIndexCreationTask<Place>
    {
        public PlacesIndex()
        {
            Map = places => places.Select(p => new
            {
                p.Name,
                Location1 = CreateSpatialField(p.Lat1, p.Lon1),
                Location2 = CreateSpatialField(p.Lat2, p.Lon2),
            });
        }
    }

    private class Place
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public double Lat1 { get; set; }
        public double Lon1 { get; set; }
        public double Lat2 { get; set; }
        public double Lon2 { get; set; }
    }
}
