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
/// RavenDB-25281: the plan-cache structural key is a SHA over a canonical serialization of the query's
/// WHERE + ORDER BY AST, with WHERE literal VALUES blanked (type kept) and parameter NAMES renumbered to
/// first-occurrence ordinals. Two queries therefore share ONE bucket iff they are structurally identical up to
/// literal values and parameter names. These tests pin that contract from the outside by counting the distinct
/// buckets in the index's SharedPlanCache (Snapshot returns one PlanCacheEntry per structural key):
///   - pure value variants and parameter-name variants collapse onto a single bucket;
///   - a different literal TYPE, operator, field, or ORDER BY shape does NOT collapse (each gets its own bucket);
/// and that the collapse is result-preserving — the one shared template resolves each query's own value through
/// the per-query slot vector, so different bound values still return their own correct results.
/// </summary>
public class PlanCacheCollapseTests : RavenTestBase
{
    public PlanCacheCollapseTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public int Seq { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Category, i.Seq };
        }
    }

    // Field names that are RQL reserved words, so referencing them must be quoted ('Order'). The parser then
    // represents the quoted field as a string ValueExpression rather than a FieldExpression - the case the
    // structural plan key has to keep distinct.
    private class Reserved
    {
        public string Id { get; set; }
        public string Order { get; set; }
        public string Group { get; set; }
    }

    private class Reserved_Index : AbstractIndexCreationTask<Reserved>
    {
        public Reserved_Index()
        {
            Map = docs => from d in docs select new { d.Order, d.Group };
            Index(x => x.Order, FieldIndexing.Search);
            Index(x => x.Group, FieldIndexing.Search);
        }
    }

    private static List<Item> BuildSeed(int count)
    {
        string[] names = { "Bob", "Alice" };
        string[] cats = { "red", "green", "blue" };
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
            items.Add(new Item { Id = $"items/{i}", Name = names[i % names.Length], Category = cats[i % cats.Length], Seq = i });
        return items;
    }

    private async Task<(IDocumentStore Store, string IndexName, List<Item> Items, PlanCache Cache)> SetupAsync(Options options)
    {
        var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(200);
        using (var bulk = store.BulkInsert())
        {
            foreach (var it in items)
                await bulk.StoreAsync(it, it.Id);
        }
        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var serverIndex = database.IndexStore.GetIndex(index.IndexName);
        var cache = ((CoraxIndexPersistence)serverIndex.IndexPersistence).SharedPlanCache;
        return (store, index.IndexName, items, cache);
    }

    private static async Task RunAsync(IAsyncDocumentSession session, string rql, params (string Name, object Value)[] parameters)
    {
        var query = session.Advanced.AsyncRawQuery<Item>(rql);
        foreach (var (name, value) in parameters)
            query.AddParameter(name, value);
        await query.ToListAsync();
    }

    // Bucket creation happens at query time (cache miss → ParseTemplate + GetOrAddBucket), so the distinct-bucket
    // count after a batch of queries is the externally visible structural-key cardinality. We measure deltas
    // around each batch so any one-off background query before the batch cannot perturb the assertion.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task ValueAndParameterNameVariantsShareBucket_TypeOperatorFieldAndOrderByDoNot(Options options)
    {
        var (store, idx, _, cache) = await SetupAsync(options);
        using (store)
        {
            int Buckets() => cache.Snapshot().Count;
            using var session = store.OpenAsyncSession();

            // 1) Pure value variants: same shape, three distinct long literals → exactly ONE bucket.
            int before = Buckets();
            await RunAsync(session, $"from index '{idx}' where Seq > 5");
            await RunAsync(session, $"from index '{idx}' where Seq > 10");
            await RunAsync(session, $"from index '{idx}' where Seq > 150");
            Assert.Equal(before + 1, Buckets());

            // 2) Parameter-name variants: same shape, different parameter names → ONE more bucket. Distinct from
            //    the literal bucket above because a parameter operand (P0) is not the same source as a literal (L).
            before = Buckets();
            await RunAsync(session, $"from index '{idx}' where Seq > $min", ("min", 5));
            await RunAsync(session, $"from index '{idx}' where Seq > $threshold", ("threshold", 20));
            Assert.Equal(before + 1, Buckets());

            // 3) Literal TYPE variant: a double literal (5.5) is L{Double}, not L{Long}. The per-variant
            //    CacheKeyHash does not see literals, so the structural key MUST keep the type → a new bucket.
            before = Buckets();
            await RunAsync(session, $"from index '{idx}' where Seq > 5.5");
            Assert.Equal(before + 1, Buckets());

            // 4) Operator variant: < vs > is a different template → a new bucket.
            before = Buckets();
            await RunAsync(session, $"from index '{idx}' where Seq < 5");
            Assert.Equal(before + 1, Buckets());

            // 5) Field variant: a different field (and value type) → a new bucket.
            before = Buckets();
            await RunAsync(session, $"from index '{idx}' where Category = 'red'");
            Assert.Equal(before + 1, Buckets());

            // 6) ORDER BY variant: the same WHERE shape as (1) but with an ORDER BY changes the template (sort
            //    metadata + sort-driven optimizations), so it must NOT collapse onto the un-ordered bucket.
            before = Buckets();
            await RunAsync(session, $"from index '{idx}' where Seq > 7 order by Seq as long");
            Assert.Equal(before + 1, Buckets());
        }
    }

    // The collapse is only safe if the single shared template resolves each query's OWN value at instantiation
    // (via the per-query slot vector), never a value baked from whichever variant compiled the plan first. This
    // runs four literal value variants and four parameter value variants through one shared bucket each, and
    // asserts every one returns its own brute-force-correct result.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task CollapsedBucketIsResultPreserving_PerQueryValuesResolveIndependently(Options options)
    {
        var (store, idx, items, cache) = await SetupAsync(options);
        using (store)
        {
            int Buckets() => cache.Snapshot().Count;
            using var session = store.OpenAsyncSession();

            int[] thresholds = { 5, 50, 150, 199 };

            List<string> Expected(int t) =>
                items.Where(i => i.Seq > t).OrderBy(i => i.Seq).Select(i => i.Id).ToList();

            // Literal value variants — all share one bucket, each must return its own correct top set.
            int before = Buckets();
            foreach (var t in thresholds)
            {
                var results = await session.Advanced
                    .AsyncRawQuery<Item>($"from index '{idx}' where Seq > {t} order by Seq as long")
                    .ToListAsync();
                Assert.Equal(Expected(t), results.Select(r => r.Id).ToList());
            }
            Assert.Equal(before + 1, Buckets()); // four distinct literals, one shared template

            // Parameter value variants — also one shared bucket (distinct from the literal one), each correct.
            before = Buckets();
            foreach (var t in thresholds)
            {
                var results = await session.Advanced
                    .AsyncRawQuery<Item>($"from index '{idx}' where Seq > $t order by Seq as long")
                    .AddParameter("t", t)
                    .ToListAsync();
                Assert.Equal(Expected(t), results.Select(r => r.Id).ToList());
            }
            Assert.Equal(before + 1, Buckets()); // four distinct parameter values, one shared template
        }
    }

    // A quoted field name (e.g. 'Order' for a reserved word) is parsed as a string ValueExpression, not a
    // FieldExpression. The structural key must still keep the field NAME: collapsing it like a value operand let
    // search('Order',$t) and search('Group',$t) share one bucket+template, so the second resolved against the
    // wrong field. This pins both halves: search-TERM variants on one quoted field still collapse (the term is a
    // slot), but a DIFFERENT quoted field does not - and each query returns its own correct, field-specific result.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task QuotedFieldName_SearchSegregatesByFieldButCollapsesTermVariants(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Reserved_Index();
        index.Execute(store);
        using (var bulk = store.BulkInsert())
        {
            await bulk.StoreAsync(new Reserved { Id = "r/1", Order = "alpha", Group = "zeta" });
            await bulk.StoreAsync(new Reserved { Id = "r/2", Order = "zeta", Group = "alpha" });
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var serverIndex = database.IndexStore.GetIndex(index.IndexName);
        var cache = ((CoraxIndexPersistence)serverIndex.IndexPersistence).SharedPlanCache;

        int Buckets() => cache.Snapshot().Count;
        using var session = store.OpenAsyncSession();

        async Task<string[]> Search(string field, object term)
        {
            var results = await session.Advanced
                .AsyncRawQuery<Reserved>($"from index '{index.IndexName}' where search('{field}', $term)")
                .AddParameter("term", term)
                .ToListAsync();
            return results.Select(r => r.Id).OrderBy(id => id).ToArray();
        }

        // Two search-TERM variants on the SAME quoted field collapse to one bucket (the term is a slot binding),
        // and each still returns its own correct result.
        int before = Buckets();
        Assert.Equal(new[] { "r/1" }, await Search("Order", "alpha"));
        Assert.Equal(new[] { "r/2" }, await Search("Order", "zeta"));
        Assert.Equal(before + 1, Buckets());

        // A DIFFERENT quoted field with the identical shape must NOT collapse onto the 'Order' bucket. Before the
        // fix the key dropped the quoted field, so this collided and returned r/1 (the Order match) instead of r/2.
        before = Buckets();
        Assert.Equal(new[] { "r/2" }, await Search("Group", "alpha"));
        Assert.Equal(before + 1, Buckets());
    }
}
