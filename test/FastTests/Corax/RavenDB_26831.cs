using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Corax.Querying.Matches;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class RavenDB_26831 : RavenTestBase
{
    public RavenDB_26831(ITestOutputHelper output) : base(output)
    {
    }

    // Compound-field numeric (long) members were encoded as raw two's-complement big-endian, which is not
    // order-preserving for negative values (negatives byte-sort above positives). A compound sorted scan /
    // open-ended range over a field with negative numeric values therefore returned wrong order and an
    // inflated TotalResults. Verify order and count are correct across a zero crossing.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CompoundSort_NegativeNumericValues_OrderedCorrectly()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new Items_ByGroupAndValue().Execute(store);

        // Values chosen to straddle zero so a non-order-preserving encoding scrambles them.
        var values = new (string Id, long Value)[]
        {
            ("items/1", -100),
            ("items/2", -16),
            ("items/3", -1),
            ("items/4", 0),
            ("items/5", 1),
            ("items/6", 16),
            ("items/7", 100),
        };

        using (var session = store.OpenSession())
        {
            foreach (var (id, value) in values)
                session.Store(new Item { Id = id, Group = "g", Value = value }, id);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        var ascendingExpected = new long[] { -100, -16, -1, 0, 1, 16, 100 };
        var descendingExpected = new long[] { 100, 16, 1, 0, -1, -16, -100 };

        using (var session = store.OpenSession())
        {
            // WhereEquals(Group) + OrderBy(Value) matches the (Group, Value) compound field and takes the
            // compound streaming-scan path where the bug lives.
            var ascending = session.Advanced.DocumentQuery<Item, Items_ByGroupAndValue>()
                .WhereEquals(x => x.Group, "g")
                .OrderBy(x => x.Value, OrderingType.Long)
                .ToList()
                .Select(x => x.Value)
                .ToArray();

            Assert.Equal(ascendingExpected, ascending);

            var descending = session.Advanced.DocumentQuery<Item, Items_ByGroupAndValue>()
                .WhereEquals(x => x.Group, "g")
                .OrderByDescending(x => x.Value, OrderingType.Long)
                .ToList()
                .Select(x => x.Value)
                .ToArray();

            Assert.Equal(descendingExpected, descending);
        }

        using (var session = store.OpenSession())
        {
            // Open-ended range crossing zero: only the four non-negative values qualify. A non-order-preserving
            // encoding both mis-orders and inflates the total (negatives leak in).
            QueryStatistics stats;
            var nonNegative = session.Advanced.DocumentQuery<Item, Items_ByGroupAndValue>()
                .Statistics(out stats)
                .WhereEquals(x => x.Group, "g")
                .AndAlso()
                .WhereGreaterThanOrEqual(x => x.Value, 0L)
                .OrderBy(x => x.Value, OrderingType.Long)
                .ToList()
                .Select(x => x.Value)
                .ToArray();

            Assert.Equal(new long[] { 0, 1, 16, 100 }, nonNegative);
            Assert.Equal(4, stats.TotalResults);
        }
    }

    // Confirms the query shape above provably takes the compound streaming-scan path
    // (DeduplicationMatch<MultiTermMatch>), i.e. OrderBy is elided in favour of the compound field.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    public async Task CompoundSort_NegativeNumericValues_UsesCompoundScanPath()
    {
        await StreamingOptimization_QueryBuilder.TestQueryBuilder<DeduplicationMatch<MultiTermMatch>, Items_ByGroupAndValue>(
            this, hasMultipleValues: false,
            session => session.Advanced.AsyncDocumentQuery<Item, Items_ByGroupAndValue>()
                .WhereEquals(x => x.Group, "g")
                .OrderBy(x => x.Value, OrderingType.Long)
                .GetIndexQuery());
    }

    // The two tests above drive the compound scan through a STRING equality (WhereEquals(Group) + OrderBy(Value)),
    // so GetStartWithTerm's numeric-term branch (the `if (t is long l)` mask, RavenDB-26831) is never actually
    // reached there - only the indexer-side encoding is exercised. Here the *equality* term itself is a long
    // (WhereEquals(Value, -16L)), which is the value the compound-scan start-with key is built from, so this
    // forces GetStartWithTerm down the `long` branch. An equality lookup only finds the right documents if the
    // query encodes -16 with the exact same order-preserving XOR mask the indexer used to store it; if the
    // query-side mask were missing or wrong, the encoded start-with key wouldn't match anything the indexer wrote
    // and the lookup would return zero results (or the wrong set).
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CompoundEquals_NegativeNumericDriver_MatchesOnlyEqualValues()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new Items_ByValueAndName().Execute(store);

        var items = new (string Id, long Value, string Name)[]
        {
            ("items/1", -100, "alice"),
            ("items/2", -16, "bob"),
            ("items/3", -16, "carol"),
            ("items/4", -16, "dave"),
            ("items/5", -1, "erin"),
            ("items/6", 0, "frank"),
            ("items/7", 16, "grace"),
            ("items/8", 100, "heidi"),
        };

        using (var session = store.OpenSession())
        {
            foreach (var (id, value, name) in items)
                session.Store(new Item { Id = id, Value = value, Name = name }, id);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            // WhereEquals(Value) + OrderBy(Name) matches the (Value, Name) compound field and takes the compound
            // streaming-scan path, with the numeric term -16 driving GetStartWithTerm's `long` branch.
            var matches = session.Advanced.DocumentQuery<Item, Items_ByValueAndName>()
                .WhereEquals(x => x.Value, -16L)
                .OrderBy(x => x.Name)
                .ToList()
                .Select(x => x.Name)
                .ToArray();

            Assert.Equal(new[] { "bob", "carol", "dave" }, matches);
        }
    }

    // Confirms the query shape above provably takes the compound streaming-scan path with a numeric equality
    // driving the scan, i.e. that GetStartWithTerm's `long` branch is actually reached via OptimizeCompoundField.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    public async Task CompoundEquals_NegativeNumericDriver_UsesCompoundScanPath()
    {
        await StreamingOptimization_QueryBuilder.TestQueryBuilder<DeduplicationMatch<MultiTermMatch>, Items_ByValueAndName>(
            this, hasMultipleValues: false,
            session => session.Advanced.AsyncDocumentQuery<Item, Items_ByValueAndName>()
                .WhereEquals(x => x.Value, -16L)
                .OrderBy(x => x.Name)
                .GetIndexQuery());
    }

    private class Item
    {
        public string Id { get; set; }
        public string Group { get; set; }
        public long Value { get; set; }
        public string Name { get; set; }
    }

    private class Items_ByGroupAndValue : AbstractIndexCreationTask<Item>
    {
        public Items_ByGroupAndValue()
        {
            Map = items => from item in items
                select new { item.Group, item.Value };

            CompoundField(x => x.Group, x => x.Value);
        }
    }

    private class Items_ByValueAndName : AbstractIndexCreationTask<Item>
    {
        public Items_ByValueAndName()
        {
            Map = items => from item in items
                select new { item.Value, item.Name };

            CompoundField(x => x.Value, x => x.Name);
        }
    }
}
