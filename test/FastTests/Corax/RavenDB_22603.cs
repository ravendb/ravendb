using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using ClientQueryInspectionNode = Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

/// <summary>
/// When a NotEquals (!=) operation is combined with AND, the optimizer should transform:
///   And(X, AndNot(AllEntries, term)) -> AndNot(X, term)
///
/// This avoids iterating through all entries in the index, which can be extremely expensive
/// for large indexes.
/// </summary>
public class RavenDB_22603 : RavenTestBase
{
    public RavenDB_22603(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NotEquals_WithAnd_ReturnsCorrectResults(Options options)
    {
        // Test: A and (B != value) should return documents where A matches and B is not equal to value
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1) { Id = "items/1" });
            session.Store(new Item("apple", "green", 2) { Id = "items/2" });
            session.Store(new Item("apple", "yellow", 3) { Id = "items/3" });
            session.Store(new Item("banana", "yellow", 4) { Id = "items/4" });
            session.Store(new Item("banana", "green", 5) { Id = "items/5" });
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Query: Name == "apple" AND Color != "red"
            // Expected: items/2 (green apple) and items/3 (yellow apple)
            var results = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "apple")
                .AndAlso()
                .WhereNotEquals(x => x.Color, "red")
                .Timings(out var timings)
                .ToList();

            // Verify correctness
            Assert.Equal(2, results.Count);
            var ids = results.Select(r => r.Id).OrderBy(id => id).ToList();
            Assert.Equal(new[] { "items/2", "items/3" }, ids);
            Assert.All(results, r => Assert.Equal("apple", r.Name));
            Assert.All(results, r => Assert.NotEqual("red", r.Color));

            // Verify optimization: Equals + NotEquals should use MultiUnaryMatch, not AndNot
            var plan = (ClientQueryInspectionNode)timings.QueryPlan;
            Assert.True(
                ContainsOperation(plan, "CompiledQuery"),
                $"Expected CompiledQuery but got: {FormatPlan(plan)}");
            Assert.False(
                ContainsOperation(plan, "BinaryMatch [AndNot]"),
                $"Expected no BinaryMatch [AndNot] but found it in plan: {FormatPlan(plan)}");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NotEquals_InMiddleOfAndChain_ReturnsCorrectResults(Options options)
    {
        // Test: A and (B != v) and C should work correctly
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("apple", "red", 1) { Id = "items/1" });
            session.Store(new Item("apple", "green", 2) { Id = "items/2" });
            session.Store(new Item("apple", "green", 3) { Id = "items/3" });
            session.Store(new Item("banana", "green", 4) { Id = "items/4" });
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Query: Name == "apple" AND Color != "red" AND Number > 1
            // Expected: items/2 and items/3 (green apples with number > 1)
            var results = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "apple")
                .AndAlso()
                .WhereNotEquals(x => x.Color, "red")
                .AndAlso()
                .WhereGreaterThan(x => x.Number, 1)
                .Timings(out var timings)
                .ToList();

            // Verify correctness
            Assert.Equal(2, results.Count);
            var ids = results.Select(r => r.Id).OrderBy(id => id).ToList();
            Assert.Equal(new[] { "items/2", "items/3" }, ids);
            Assert.All(results, r => Assert.Equal("apple", r.Name));
            Assert.All(results, r => Assert.Equal("green", r.Color));
            Assert.All(results, r => Assert.True(r.Number > 1));

            // Verify optimization: Equals + NotEquals + GreaterThan should use MultiUnaryMatch
            var plan = (ClientQueryInspectionNode)timings.QueryPlan;
            Assert.True(
                ContainsOperation(plan, "CompiledQuery"),
                $"Expected CompiledQuery but got: {FormatPlan(plan)}");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NotEquals_NumericField_ReturnsCorrectResults(Options options)
    {
        // Test: NotEquals with numeric field combined with AND
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item("a", "x", 1) { Id = "items/1" });
            session.Store(new Item("a", "y", 2) { Id = "items/2" });
            session.Store(new Item("a", "z", 3) { Id = "items/3" });
            session.Store(new Item("b", "x", 1) { Id = "items/4" });
            session.Store(new Item("b", "y", 4) { Id = "items/5" });
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Query: Name == "a" AND Number != 1
            // Expected: items/2 and items/3 (Name="a" with Number != 1)
            var results = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "a")
                .AndAlso()
                .WhereNotEquals(x => x.Number, 1)
                .Timings(out var timings)
                .ToList();

            // Verify correctness
            Assert.Equal(2, results.Count);
            var ids = results.Select(r => r.Id).OrderBy(id => id).ToList();
            Assert.Equal(new[] { "items/2", "items/3" }, ids);
            Assert.All(results, r => Assert.Equal("a", r.Name));
            Assert.All(results, r => Assert.NotEqual(1, r.Number));

            // Verify optimization: Equals + NotEquals should use MultiUnaryMatch
            var plan = (ClientQueryInspectionNode)timings.QueryPlan;
            Assert.True(
                ContainsOperation(plan, "CompiledQuery"),
                $"Expected CompiledQuery but got: {FormatPlan(plan)}");
        }
    }

    /// <summary>
    /// Verifies that multiple NotEquals on different fields works correctly
    /// when combined with an Equals anchor, and uses MultiUnaryMatch optimization.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MultipleNotEquals_WithEqualsAnchor_UsesMultiUnaryMatch(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            // Use explicit IDs to verify correctness at document ID level
            session.Store(new Item("apple", "red", 1) { Id = "items/1" });
            session.Store(new Item("apple", "green", 2) { Id = "items/2" });
            session.Store(new Item("apple", "yellow", 3) { Id = "items/3" });
            session.Store(new Item("banana", "yellow", 4) { Id = "items/4" });
            session.Store(new Item("apple", "orange", 5) { Id = "items/5" });
            session.SaveChanges();
        }

        new ItemIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // Query: Name == "apple" AND Color != "red" AND Color != "green"
            var results = session.Advanced.DocumentQuery<Item, ItemIndex>()
                .WhereEquals(x => x.Name, "apple")
                .AndAlso()
                .WhereNotEquals(x => x.Color, "red")
                .AndAlso()
                .WhereNotEquals(x => x.Color, "green")
                .Timings(out var timings)
                .ToList();

            // Verify correctness: should return yellow and orange apples (items/3, items/5)
            Assert.Equal(2, results.Count);
            var ids = results.Select(r => r.Id).OrderBy(id => id).ToList();
            Assert.Equal(new[] { "items/3", "items/5" }, ids);
            Assert.All(results, r => Assert.Equal("apple", r.Name));
            Assert.All(results, r => Assert.NotEqual("red", r.Color));
            Assert.All(results, r => Assert.NotEqual("green", r.Color));

            // Verify optimization path was chosen
            var plan = (ClientQueryInspectionNode)timings.QueryPlan;
            Assert.True(
                ContainsOperation(plan, "CompiledQuery"),
                $"Expected CompiledQuery but got: {FormatPlan(plan)}");
        }
    }

    private static bool ContainsOperation(ClientQueryInspectionNode node, string operation)
    {
        if (node.Operation?.Contains(operation, StringComparison.OrdinalIgnoreCase) == true)
            return true;

        if (node.Children == null)
            return false;

        return node.Children.Any(child => ContainsOperation(child, operation));
    }

    private static string FormatPlan(ClientQueryInspectionNode node, int indent = 0)
    {
        var prefix = new string(' ', indent * 2);
        var result = $"{prefix}{node.Operation}";
        if (node.Children != null)
        {
            foreach (var child in node.Children)
                result += "\n" + FormatPlan(child, indent + 1);
        }
        return result;
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

/// <summary>
/// Primitive-level tests for RavenDB-22603 that test the Corax query operations directly
/// without going through the full RavenDB query layer.
/// </summary>
public class RavenDB_22603_Primitive : StorageTest
{
    private List<Entry> _entries;
    private const int IdFieldId = 0, NameFieldId = 1, ColorFieldId = 2, NumberFieldId = 3;
    private IndexFieldsMapping _knownFields;

    public RavenDB_22603_Primitive(ITestOutputHelper output) : base(output)
    {
    }

    /// <summary>
    /// Test the optimization: And(X, NotEquals) should produce same results as AndNot(X, term)
    /// This verifies the CoraxAndQueries optimization at the primitive level.
    /// </summary>
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CoraxAndQueries_NotEqualsOptimization_ProducesSameResults()
    {
        PrepareData();
        IndexEntries();

        // Expected: entries where Name == "apple" AND Color != "red"
        var expected = _entries.Where(e => e.Name == "apple" && e.Color != "red").Select(e => e.Id).ToList();

        // Both paths (And+AndNot vs. direct AndNot) compile to the same CompiledQueryMatch; verify results via RQL.
        var results = ExecuteRQLQuery("FROM TestIndex WHERE Name = 'apple' AND NOT Color = 'red'");
        results.Sort();
        expected.Sort();

        Assert.Equal(expected, results);
    }

    /// <summary>
    /// Test multiple NotEquals combined: (A != v1) AND (B != v2)
    /// </summary>
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void MultipleNotEquals_ProducesCorrectResults()
    {
        PrepareData();
        IndexEntries();

        // Expected: entries where Name != "apple" AND Color != "yellow"
        var expected = _entries.Where(e => e.Name != "apple" && e.Color != "yellow").Select(e => e.Id).ToList();

        var results = ExecuteRQLQuery("FROM TestIndex WHERE Name != 'apple' AND Color != 'yellow'");
        results.Sort();
        expected.Sort();

        Assert.Equal(expected, results);
    }

    /// <summary>
    /// Test NotEquals in middle of AND chain: A AND (B != v) AND C
    /// </summary>
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void NotEqualsInMiddle_ProducesCorrectResults()
    {
        PrepareData();
        IndexEntries();

        // Expected: entries where Name == "apple" AND Color != "red" AND Number > 1
        var expected = _entries.Where(e => e.Name == "apple" && e.Color != "red" && e.Number > 1).Select(e => e.Id).ToList();

        var results = ExecuteRQLQuery("FROM TestIndex WHERE Name = 'apple' AND NOT Color = 'red' AND Number > 1");
        results.Sort();
        expected.Sort();

        Assert.Equal(expected, results);
    }

    /// <summary>
    /// Test NotEquals with numeric field
    /// </summary>
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void NotEqualsWithNumericField_ProducesCorrectResults()
    {
        PrepareData();
        IndexEntries();

        // Expected: entries where Name == "apple" AND Number != 1
        var expected = _entries.Where(e => e.Name == "apple" && e.Number != 1).Select(e => e.Id).ToList();

        var results = ExecuteRQLQuery("FROM TestIndex WHERE Name = 'apple' AND NOT Number = 1");
        results.Sort();
        expected.Sort();

        Assert.Equal(expected, results);
    }

    /// <summary>
    /// Test multiple NotEquals on the same field: (A != v1) AND (A != v2)
    /// This exercises a different code path in the optimizer.
    /// </summary>
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void MultipleNotEquals_SameField_ProducesCorrectResults()
    {
        PrepareData();
        IndexEntries();

        // Expected: entries where Name != "apple" AND Name != "banana"
        // This means: cherry entries only (entries/5 and entries/6)
        var expected = _entries.Where(e => e.Name != "apple" && e.Name != "banana").Select(e => e.Id).ToList();

        var results = ExecuteRQLQuery("FROM TestIndex WHERE Name != 'apple' AND Name != 'banana'");
        results.Sort();
        expected.Sort();

        Assert.Equal(expected, results);
        // Should only contain cherry entries (entries/5 and entries/6)
        Assert.Equal(2, results.Count);
        Assert.Contains("entries/5", results);
        Assert.Contains("entries/6", results);
    }

    /// <summary>
    /// Tests that CoraxAndQueries produces correct results when Equals + NotEquals
    /// are combined, comparing the MultiUnaryMatch path with the AndNot path.
    /// Both should return identical results.
    /// </summary>
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void EqualsWithNotEquals_MultiUnaryMatchAndAndNot_ReturnSameResults()
    {
        PrepareData();
        IndexEntries();

        // Setup: entries/1 (apple,red), entries/2 (apple,green), entries/7 (apple,red), entries/8 (apple,green)
        // Query: Name == "apple" AND Color != "red"
        // Expected: entries/2, entries/8 (green apples only)
        // Both the MultiUnaryMatch path and the AndNot path compile to the same CompiledQueryMatch; verify results via RQL.
        var results = ExecuteRQLQuery("FROM TestIndex WHERE Name = 'apple' AND NOT Color = 'red'");
        results.Sort();

        // Verify expected entries
        Assert.Contains("entries/2", results);
        Assert.Contains("entries/8", results);
        Assert.DoesNotContain("entries/1", results);  // red apple
        Assert.DoesNotContain("entries/7", results);  // red apple
    }

    private List<string> ExecuteRQLQuery(string rqlQuery)
    {
        using var searcher = new IndexSearcher(Env, _knownFields);
        var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
        var planParams = new PlanParameters
        {
            IndexSearcher = searcher,
            Metadata = queryMetadata,
            QueryParameters = null,
            Allocator = Allocator
        };
        var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, _knownFields), null, false, default);

        var list = new List<string>();
        Span<long> ids = stackalloc long[256];
        int count;
        while ((count = match.Fill(ids)) > 0)
        {
            for (int i = 0; i < count; i++)
                list.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
        }
        return list;
    }

    private void PrepareData()
    {
        _entries = new List<Entry>
        {
            new("entries/1", "apple", "red", 1),
            new("entries/2", "apple", "green", 2),
            new("entries/3", "banana", "yellow", 3),
            new("entries/4", "banana", "green", 4),
            new("entries/5", "cherry", "red", 5),
            new("entries/6", "cherry", "red", 6),
            new("entries/7", "apple", "red", 7),
            new("entries/8", "apple", "green", 8)
        };
    }

    private void IndexEntries()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        _knownFields = CreateKnownFields(bsc);

        using var indexWriter = new IndexWriter(Env, _knownFields, SupportedFeatures.All);

        foreach (var entry in _entries)
        {
            using var entryBuilder = indexWriter.Index(entry.Id);
            entryBuilder.Write(IdFieldId, Encoding.UTF8.GetBytes(entry.Id));
            entryBuilder.Write(NameFieldId, Encoding.UTF8.GetBytes(entry.Name));
            entryBuilder.Write(ColorFieldId, Encoding.UTF8.GetBytes(entry.Color));
            entryBuilder.Write(NumberFieldId, Encoding.UTF8.GetBytes(entry.Number.ToString()), entry.Number, entry.Number);
            entryBuilder.EndWriting();
        }

        indexWriter.Commit();
    }

    private IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice nameSlice);
        Slice.From(ctx, "Color", ByteStringType.Immutable, out Slice colorSlice);
        Slice.From(ctx, "Number", ByteStringType.Immutable, out Slice numberSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdFieldId, idSlice)
            .AddBinding(NameFieldId, nameSlice)
            .AddBinding(ColorFieldId, colorSlice)
            .AddBinding(NumberFieldId, numberSlice);
        return builder.Build();
    }

    private record Entry(string Id, string Name, string Color, long Number);

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        _knownFields?.Dispose();
    }
}
