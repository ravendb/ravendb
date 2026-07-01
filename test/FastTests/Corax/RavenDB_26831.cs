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

    private class Item
    {
        public string Id { get; set; }
        public string Group { get; set; }
        public long Value { get; set; }
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
}
