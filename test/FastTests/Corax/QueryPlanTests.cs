using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class QueryPlanTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void QueryPlanForMultiUnaryMatch(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .AndAlso()
            .WhereBetween(x => x.Date, new DateTime(2024, 8, 21), new DateTime(2024, 8, 23))
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.Equal(1, result.Count);
        Assert.NotNull(timings);
        Assert.NotNull(timings.QueryPlan);
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = docs => from d in docs
                select new { d.Name, d.Date };
        }
    }

    
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DecisionTrailSurfacedInTimings_NoOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.NotNull(plan.Parameters);
        // A plain equals query with no ORDER BY makes no cost-gated strategy decision: the absence of
        // ORDER BY is a precondition, not a decision, so the DecisionTrail records nothing and no
        // DecisionTrail node is emitted. The chosen strategy is still surfaced via OptimizationHint.
        Assert.True(plan.Parameters.ContainsKey("OptimizationHint"));
        var trailNode = plan.Children?.FirstOrDefault(c => c.Operation == "DecisionTrail");
        Assert.Null(trailNode);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DecisionTrailSurfacedInTimings_WithOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .AndAlso()
            .WhereBetween(x => x.Date, new DateTime(2024, 8, 21), new DateTime(2024, 8, 23))
            .OrderBy(x => x.Date)
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
        Assert.NotNull(plan);
        // With ORDER BY, the plan root is the SortingMatch wrapper, and the compiled-query
        // inspection (which carries the DecisionTrail) is its single child.
        var compiledRoot = plan.Operation == "CompiledQuery"
            ? plan
            : plan.Children?.FirstOrDefault(c => c.Operation == "CompiledQuery");
        Assert.NotNull(compiledRoot);
        var trailNode = compiledRoot.Children?.FirstOrDefault(c => c.Operation == "DecisionTrail");
        Assert.NotNull(trailNode);
        Assert.True(trailNode.Children.Count >= 2);
        foreach (var child in trailNode.Children)
        {
            Assert.True(child.Parameters.ContainsKey("Accepted"));
            Assert.True(child.Parameters.ContainsKey("Reason"));
        }
        var acceptedEntries = trailNode.Children.Where(c => c.Parameters["Accepted"] == "True").ToList();
        Assert.True(acceptedEntries.Count >= 1);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TimingIsOverlaidOnStructuralPlan_ForCompiledMatch(Options options)
    {
        // A term query runs through CompiledQueryMatch, so OverlayTimings annotates the structural plan
        // with the run's telemetry: Output on the CompiledQuery node, and OutputWithDups/Ms on the executed op.
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .Timings(out var timings)
            .ToList();

        Assert.Equal(1, result.Count);
        var compiledRoot = CompiledRoot(timings);
        Assert.NotNull(compiledRoot);
        // timing overlay
        Assert.True(compiledRoot.Parameters.ContainsKey("Output"));
        var fill = compiledRoot.Children.First(c => c.Operation == "FillFromPostingSource");
        Assert.True(fill.Parameters.ContainsKey("Ms"));
        Assert.True(fill.Parameters.ContainsKey("OutputWithDups"));
        // structural data sits alongside it
        Assert.Equal("Name", fill.Parameters["FieldName"]);
        Assert.Equal("maciej", fill.Parameters["Term"]);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SpatialOnlyPlan_IsStructural_WithNoTimingTelemetry(Options options)
    {
        // A spatial-only query takes the all-entries post-filter bypass: its executed match is a
        // PostFilterMatch (not a CompiledQueryMatch), so there is NO timing telemetry to overlay.
        // The structural plan must still be fully formed — a uniform op tree (Fill + SpatialMatch),
        // NOT a raw match dump — proving the plan is built independently of timing data.
        using var store = GetDocumentStore(options);
        new SpatialIndex().Execute(store);
        using var session = store.OpenSession();
        session.Store(new SpatialDto { Name = "alpha", Lat = 0, Lng = 0 });
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var spatial = session.Advanced
            .DocumentQuery<SpatialDto>("SpatialIndex")
            .Spatial("Coordinates", factory => factory.WithinRadius(60, 0, 0, Raven.Client.Documents.Indexes.Spatial.SpatialUnits.Miles))
            .Timings(out var timings)
            .ToList();

        Assert.Equal(1, spatial.Count);
        var compiledRoot = CompiledRoot(timings);
        Assert.NotNull(compiledRoot);
        // uniform op tree
        Assert.Contains(compiledRoot.Children, c => c.Operation == "FillFromMatch");
        Assert.Contains(compiledRoot.Children, c => c.Operation.Contains("Spatial"));
        // The post-filter match surfaces its final survivor count as the Result output (1 stored doc),
        // even though no CompiledQueryMatch ran. But per-op timing telemetry is still NOT overlaid:
        Assert.Equal("1", compiledRoot.Parameters["Output"]);
        var fill = compiledRoot.Children.First(c => c.Operation == "FillFromMatch");
        Assert.False(fill.Parameters.ContainsKey("Ms"));
        Assert.False(fill.Parameters.ContainsKey("Output"));
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void PlanBuildSpanSurfacedUnderCoraxScope(Options options)
    {
        // The plan-build / IL-compile work composes as an "Optimizer" span under the "Corax" timing scope.
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(timings.Timings);
        var corax = FindTiming(timings, "Corax");
        Assert.NotNull(corax);
        Assert.NotNull(corax.Timings);
        Assert.True(corax.Timings.ContainsKey("Optimizer"));
    }

    private static Raven.Client.Documents.Queries.Timings.QueryInspectionNode CompiledRoot(Raven.Client.Documents.Queries.Timings.QueryTimings timings)
    {
        var plan = timings.QueryPlan as Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
        if (plan == null)
            return null;
        return plan.Operation == "CompiledQuery"
            ? plan
            : plan.Children?.FirstOrDefault(c => c.Operation == "CompiledQuery");
    }

    private static Raven.Client.Documents.Queries.Timings.QueryTimings FindTiming(Raven.Client.Documents.Queries.Timings.QueryTimings node, string key)
    {
        if (node.Timings == null)
            return null;
        if (node.Timings.TryGetValue(key, out var found))
            return found;
        foreach (var child in node.Timings.Values)
        {
            var hit = FindTiming(child, key);
            if (hit != null)
                return hit;
        }
        return null;
    }

    private class SpatialIndex : AbstractIndexCreationTask<SpatialDto>
    {
        public SpatialIndex()
        {
            Map = docs => from d in docs
                select new { d.Name, Coordinates = CreateSpatialField(d.Lat, d.Lng) };
        }
    }

    private class SpatialDto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public double Lat { get; set; }
        public double Lng { get; set; }
    }

    private record Dto(string Name, DateTime Date, string Id = null);
}
