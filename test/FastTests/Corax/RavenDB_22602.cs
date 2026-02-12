using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using ClientQueryInspectionNode = Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

/// <summary>
/// RavenDB-22602: Optimize true OR and true AND queries in CORAX
///
/// Boolean logic optimizations:
/// - true OR X → AllEntries (matches all documents)
/// - X OR true → AllEntries (matches all documents)
/// - true AND X → X (simplifies to X)
/// - X AND true → X (simplifies to X)
/// - Cascading: (true OR X) OR Y → AllEntries (optimization propagates)
///
/// These patterns occur in production queries and should be simplified to avoid
/// severe performance degradation.
/// </summary>
public class RavenDB_22602 : RavenTestBase
{
    public RavenDB_22602(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TrueAndOr_BasicCases(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1) { Id = "items/1" });
            session.Store(new Item("banana", "yellow", 2) { Id = "items/2" });
            session.Store(new Item("apple", "green", 3) { Id = "items/3" });
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Baseline plans (without true)
            var termBaseline = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "apple")
                .Timings(out var termTimings)
                .ToList();
            var termPlan = (ClientQueryInspectionNode)termTimings.QueryPlan;

            var allBaseline = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .Timings(out var allTimings)
                .ToList();
            var allPlan = (ClientQueryInspectionNode)allTimings.QueryPlan;

            // true AND X → X: plan should match Name = 'apple' baseline
            var andResults = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where true and Name = 'apple' include timings()")
                .Timings(out var andTimings).ToList();
            Assert.Equal(2, andResults.Count);
            Assert.All(andResults, r => Assert.Equal("apple", r.Name));
            AssertSamePlan(termPlan, andTimings, "true AND X should produce same plan as X");

            // X AND true → X: plan should match Name = 'apple' baseline
            var andResults2 = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where Name = 'apple' and true include timings()")
                .Timings(out var andTimings2).ToList();
            Assert.Equal(2, andResults2.Count);
            AssertSamePlan(termPlan, andTimings2, "X AND true should produce same plan as X");

            // true OR X → AllEntries: plan should match no-where-clause baseline
            var orResults = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where true or Name = 'apple' include timings()")
                .Timings(out var orTimings).ToList();
            Assert.Equal(3, orResults.Count);
            AssertSamePlan(allPlan, orTimings, "true OR X should produce same plan as AllEntries");

            // X OR true → AllEntries: plan should match no-where-clause baseline
            var orResults2 = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where Name = 'apple' or true include timings()")
                .Timings(out var orTimings2).ToList();
            Assert.Equal(3, orResults2.Count);
            AssertSamePlan(allPlan, orTimings2, "X OR true should produce same plan as AllEntries");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TrueAnd_WithNegation(Options options)
    {
        // Guard condition: NegatedExpression takes the existing NOT path, not the true AND shortcut
        // Correctness-only is appropriate since the optimization is intentionally not applied here
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1));
            session.Store(new Item("banana", "yellow", 2));
            session.Store(new Item("cherry", "green", 3));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results1 = session.Advanced.RawQuery<Item>("from index 'ItemIndex' where true and Name != 'apple'").ToList();
            Assert.Equal(2, results1.Count);
            Assert.All(results1, r => Assert.NotEqual("apple", r.Name));

            var results2 = session.Advanced.RawQuery<Item>("from index 'ItemIndex' where Name != 'apple' and true").ToList();
            Assert.Equal(2, results2.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NestedExpressions(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "green", 1));
            session.Store(new Item("banana", "red", 3));
            session.Store(new Item("cherry", "yellow", 5));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // (true OR X) AND Y: the OR is optimized to AllEntries at the AST level,
            // but AllEntries AND Y → Y doesn't collapse further because AllEntries is
            // already materialized (MemoizationMatch) by the time AND is built.
            // Correctness-only: verify the right documents are returned.
            var r1 = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where (true or Name = 'apple') and Number > 2").ToList();
            Assert.Equal(2, r1.Count);
            Assert.All(r1, r => Assert.True(r.Number > 2));

            // (true AND X) OR Y: the true AND collapses at AST level, so the plan
            // should match X OR Y exactly
            var orBaseline = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "apple")
                .OrElse()
                .WhereEquals(x => x.Color, "red")
                .Timings(out var orTimings)
                .ToList();
            var orPlan = (ClientQueryInspectionNode)orTimings.QueryPlan;

            var r2 = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where (true and Name = 'apple') or Color = 'red' include timings()")
                .Timings(out var r2Timings).ToList();
            Assert.Equal(2, r2.Count);
            AssertSamePlan(orPlan, r2Timings, "(true AND X) OR Y should produce same plan as X OR Y");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CascadingOr_Optimization(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1));
            session.Store(new Item("banana", "yellow", 5));
            session.Store(new Item("cherry", "green", 10));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // AllEntries baseline for r1/r2
            _ = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .Timings(out var allTimings)
                .ToList();
            var allPlan = (ClientQueryInspectionNode)allTimings.QueryPlan;

            // Number > 2 baseline for r3
            _ = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereGreaterThan(x => x.Number, 2)
                .Timings(out var numberBaselineTimings)
                .ToList();
            var numberBaseline = (ClientQueryInspectionNode)numberBaselineTimings.QueryPlan;

            // r1: (true OR X) OR Y — the inner OR returns MemoizationMatch(AllEntries);
            // CoraxOrQueries.TryAddItem recognises it via IsAllEntries and sets _hasAllEntries,
            // and the OR call-site immediately replays AllEntries instead of returning CoraxOrQueries.
            var r1 = session.Advanced.RawQuery<Item>(
                    "from index 'ItemIndex' where true or Name = 'apple' or Color = 'red' include timings()")
                .Timings(out var r1Timings).ToList();
            Assert.Equal(3, r1.Count);
            AssertSamePlan(allPlan, r1Timings, "true OR X OR Y should produce same plan as AllEntries");

            // r2: X OR true OR Y — same optimisation, middle position
            var r2 = session.Advanced.RawQuery<Item>(
                    "from index 'ItemIndex' where Name = 'apple' or true or Color = 'yellow' include timings()")
                .Timings(out var r2Timings).ToList();
            Assert.Equal(3, r2.Count);
            AssertSamePlan(allPlan, r2Timings, "X OR true OR Y should produce same plan as AllEntries");

            // r3: ((true OR X) OR Y) AND Z — the OR now returns MemoizationMatch(AllEntries)
            // instead of CoraxOrQueries, so IsAllEntries recognises it in the AND default case
            // and the whole expression collapses to Z (Number > 2).
            var r3 = session.Advanced.RawQuery<Item>(
                    "from index 'ItemIndex' where ((true or Name = 'apple') or Color = 'red') and Number > 2 include timings()")
                .Timings(out var r3Timings).ToList();
            Assert.Equal(2, r3.Count);
            Assert.All(r3, r => Assert.True(r.Number > 2));
            AssertSamePlan(numberBaseline, r3Timings, "((true OR X) OR Y) AND Z should produce same plan as Z");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CascadingOrInsideAnd_ShouldMatchBaselinePlan(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1));
            session.Store(new Item("banana", "yellow", 5));
            session.Store(new Item("cherry", "green", 10));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Baseline: Number > 2
            _ = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereGreaterThan(x => x.Number, 2)
                .Timings(out var baselineTimings)
                .ToList();
            var baselinePlan = (ClientQueryInspectionNode)baselineTimings.QueryPlan;

            // (true OR Name='apple') AND Number > 2 should collapse to Number > 2
            _ = session.Advanced.RawQuery<Item>(
                    "from index 'ItemIndex' where (true or Name = 'apple') and Number > 2 include timings()")
                .Timings(out var optimizedTimings)
                .ToList();

            AssertSamePlan(baselinePlan, optimizedTimings,
                "(true OR X) AND Y should produce same plan as Y");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IntegrationWith_RavenDB22603(Options options)
    {
        // Verify both optimizations work together
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1));
            session.Store(new Item("apple", "green", 2));
            session.Store(new Item("cherry", "yellow", 3));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Baseline: Name = 'apple' AND Color != 'red' (without the true AND wrapper)
            var baseline = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "apple")
                .AndAlso()
                .WhereNotEquals(x => x.Color, "red")
                .Timings(out var baseTimings)
                .ToList();
            var basePlan = (ClientQueryInspectionNode)baseTimings.QueryPlan;

            // (true AND Name = 'apple') AND Color != 'red'
            var results = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where (true and Name = 'apple') and Color != 'red' include timings()")
                .Timings(out var timings).ToList();

            Assert.Single(results);
            Assert.Equal("green", results[0].Color);
            AssertSamePlan(basePlan, timings, "(true AND X) AND Y should produce same plan as X AND Y");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void StreamingOptimization_WithTrueAnd(Options options)
    {
        // "X AND true" should produce the same query plan as just "X"
        // This validates that the true AND optimization preserves streaming optimization
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 10));
            session.Store(new Item("apricot", "orange", 20));
            session.Store(new Item("avocado", "green", 30));
            session.Store(new Item("banana", "yellow", 40));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Baseline: startsWith(Name, 'a') order by Name — should use streaming
            var baseline = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereStartsWith(x => x.Name, "a")
                .OrderBy(x => x.Name)
                .Timings(out var baselineTimings)
                .ToList();

            Assert.Equal(3, baseline.Count);
            var baselinePlan = (ClientQueryInspectionNode)baselineTimings.QueryPlan;
            Assert.NotNull(baselinePlan);

            // Optimized: startsWith(Name, 'a') AND true order by Name — should produce same plan
            var optimized = session.Advanced.RawQuery<Item>(
                "from index 'ItemIndex' where startsWith(Name, 'a') and true order by Name include timings()")
                .Timings(out var optimizedTimings)
                .ToList();

            Assert.Equal(3, optimized.Count);
            AssertSamePlan(baselinePlan, optimizedTimings, "startsWith AND true ORDER BY should produce same plan as startsWith ORDER BY");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void StreamingOptimization_WithTrueAndOnLeft_ShouldMatchBaselinePlan(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 10));
            session.Store(new Item("apricot", "orange", 20));
            session.Store(new Item("avocado", "green", 30));
            session.Store(new Item("banana", "yellow", 40));
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            _ = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereStartsWith(x => x.Name, "a")
                .OrderBy(x => x.Name)
                .Timings(out var baselineTimings)
                .ToList();
            var baselinePlan = (ClientQueryInspectionNode)baselineTimings.QueryPlan;

            _ = session.Advanced.RawQuery<Item>(
                    "from index 'ItemIndex' where true and startsWith(Name, 'a') order by Name include timings()")
                .Timings(out var optimizedTimings)
                .ToList();

            AssertSamePlan(baselinePlan, optimizedTimings,
                "true AND startsWith ORDER BY should produce same plan as startsWith ORDER BY");
        }
    }

    private static void AssertSamePlan(ClientQueryInspectionNode expected, QueryTimings actual, string message)
    {
        var actualPlan = (ClientQueryInspectionNode)actual.QueryPlan;
        Assert.NotNull(actualPlan);
        Assert.Equal(FormatPlan(expected), FormatPlan(actualPlan));
    }

    private static string FormatPlan(ClientQueryInspectionNode node, int indent = 0)
    {
        if (node == null)
            return "<null>";

        var prefix = new string(' ', indent * 2);
        var line = $"{prefix}{node.Operation}";
        if (node.Parameters is { Count: > 0 })
            line += $" ({string.Join(", ", node.Parameters.Select(p => $"{p.Key}={p.Value}"))})";

        if (node.Children == null)
            return line;

        return line + "\n" + string.Join("\n", node.Children.Select(c => FormatPlan(c, indent + 1)));
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Color { get; set; }
        public int Number { get; set; }

        public Item()
        {
        }

        public Item(string name, string color, int number)
        {
            Name = name;
            Color = color;
            Number = number;
        }
    }

    private class ItemIndex : AbstractIndexCreationTask<Item>
    {
        public ItemIndex()
        {
            Map = items => from item in items
                select new
                {
                    item.Name,
                    item.Color,
                    item.Number
                };
        }
    }
}
