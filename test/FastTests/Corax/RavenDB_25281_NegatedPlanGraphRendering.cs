using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace FastTests.Corax;

/// <summary>
/// The query-plan graph must show a negated post-filter (<c>not spatial.within(...)</c>) as a SUBTRACTION, not as
/// an ordinary intersecting post-filter — otherwise the plan for a negated query is drawn identically to its
/// positive counterpart. NegatedPostFilterMatch.Inspect marks each subtracted clause node Negated=true, and
/// QueryPlanGraph renders it distinctly (label + colour). This pins that so the negation cannot silently vanish.
/// </summary>
public class RavenDB_25281_NegatedPlanGraphRendering : RavenTestBase
{
    public RavenDB_25281_NegatedPlanGraphRendering(ITestOutputHelper output) : base(output)
    {
    }

    private class Place
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
    }

    private class PlacesIndex : AbstractIndexCreationTask<Place>
    {
        public PlacesIndex()
        {
            Map = places => places.Select(p => new { p.Name, p.Tag, Location = CreateSpatialField(p.Lat, p.Lon) });
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Negated_post_filter_is_rendered_distinctly_in_the_plan_graph()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var s = store.OpenSession())
        {
            s.Store(new Place { Name = "Inside-Keep", Tag = "keep", Lat = 0, Lon = 0 });
            s.Store(new Place { Name = "Outside-Keep", Tag = "keep", Lat = 30, Lon = 30 });
            s.SaveChanges();
        }
        new PlacesIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        const string circle = "spatial.circle(60, 0, 0, 'miles')";

        string Dot(string rql)
        {
            session.Advanced.RawQuery<Place>(rql + " include timings()")
                .WaitForNonStaleResults()
                .Timings(out var timings)
                .ToList();
            return ((QueryInspectionNode)timings.QueryPlan).Parameters["PlanGraphDot"];
        }

        var negated = Dot($"from index 'PlacesIndex' where Tag = 'keep' and not spatial.within(Location, {circle})");
        var positive = Dot($"from index 'PlacesIndex' where Tag = 'keep' and spatial.within(Location, {circle})");

        // Both render the spatial post-filter node.
        Assert.Contains("SpatialMatch", negated);
        Assert.Contains("SpatialMatch", positive);

        // The negated one is marked as a subtraction; the positive one is not.
        Assert.Contains("data_negated=\"true\"", negated);
        Assert.Contains("NEGATED (excluded)", negated);
        Assert.DoesNotContain("data_negated=\"true\"", positive);
        Assert.DoesNotContain("NEGATED (excluded)", positive);
    }
}
