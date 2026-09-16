using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Exercises the IN / ALL IN residual predicate on the entry-scan path. Each test shapes
/// the data so the entry-scan heuristic fires: a highly selective seed clause (~10 docs)
/// is AND'd with an IN clause whose value set has a large posting list (~3000 docs via
/// filler), satisfying <c>seed.Count * 64 &lt; inMatch.Count</c>. The IN/AllIn clause then
/// becomes a per-entry residual predicate rather than a bitmap posting-list union, which is
/// the code path added for the entry-scan IN support.
/// </summary>
public class InEntryScanTests : RavenTestBase
{
    public InEntryScanTests(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    private const int FillerCount = 3000;

    private static async Task SeedAsync(Raven.Client.Documents.IDocumentStore store)
    {
        using var session = store.OpenAsyncSession();

        // 10 selective "rare" seed docs with assorted attribute values.
        for (int i = 0; i < 10; i++)
        {
            await session.StoreAsync(new Item
            {
                Seed = "rare",
                Color = i % 2 == 0 ? null : (i % 3 == 0 ? "red" : ColorCycle(i)),
                Code = i,
                Score = i * 1.5,
                Tags = i % 2 == 0 ? new[] { "x", "y", "z" } : new[] { "x", "z" }
            });
        }

        // Filler docs that DO satisfy the IN value sets (Color=blue, Code=500, Score=999.5,
        // Tags⊇{x,y}) so the IN/AllIn posting lists are large and entry scan wins the cost check —
        // but they never satisfy Seed='rare', so they are excluded from results.
        for (int i = 0; i < FillerCount; i++)
        {
            await session.StoreAsync(new Item
            {
                Seed = $"common-{i % 50}",
                Color = "blue",
                Code = 500,
                Score = 999.5,
                Tags = new[] { "x", "y", "common" }
            });
        }

        await session.SaveChangesAsync();
    }

    // rare docs: i even -> null, i==3,9 -> "red", others -> green/blue cycle
    private static string ColorCycle(int i) => (i % 3) switch { 1 => "green", _ => "blue" };

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task InString_MultiTerm_EntryScan_ReturnsCorrectResults()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs Colors: i0=null,i1=green,i2=null,i3=red,i4=null,i5=blue,i6=null,i7=green,i8=null,i9=red
        // IN ('green','blue') over rare docs -> i1(green), i5(blue), i7(green) => 3
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Color in ('green', 'blue')")
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.True(r.Color is "green" or "blue");
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task InLong_EntryScan_ReturnsCorrectResults()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs Code = i (0..9). IN [2,5,7,500] -> rare matching 2,5,7 => 3 (no rare has 500).
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Code in (2, 5, 7, 500)")
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.Contains(r.Code, new long[] { 2, 5, 7 });
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task InDouble_EntryScan_ReturnsCorrectResults()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs Score = i*1.5. IN [3.0, 7.5, 12.0, 999.5] -> i2(3.0), i5(7.5), i8(12.0) => 3.
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Score in (3.0, 7.5, 12.0, 999.5)")
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.Contains(r.Score, new[] { 3.0, 7.5, 12.0 });
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task AllIn_MultiValue_EntryScan_ReturnsCorrectResults()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs: even i -> Tags=[x,y,z] (covers {x,y}); odd i -> Tags=[x,z] (missing y).
        // ALL IN (x,y) over rare docs -> even i = 0,2,4,6,8 => 5.
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Tags all in ('x', 'y')")
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.Contains("x", r.Tags);
            Assert.Contains("y", r.Tags);
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task InWithNull_EntryScan_MatchesNullField()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // 'blue' is included so the filler docs (all Color='blue') bloat the IN posting list and
        // entry scan fires. rare docs: even i -> Color=null (5 docs); i5 -> Color=blue (1 doc).
        // IN ('blue', null) over rare docs => 5 null + 1 blue = 6. The null match exercises the
        // HasNull flag carried into the residual scan via ResidualInValues.HasNull.
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Color in ('blue', null)")
            .ToListAsync();

        Assert.Equal(6, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.True(r.Color is null or "blue");
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task In_EntryScan_MatchesOrChain_Parity()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // IN clause: entry-scan residual path (large filler posting list).
        var inResults = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Color in ('green', 'blue')")
            .ToListAsync();

        // Equivalent OR chain: bitmap pipeline. Result sets must be identical.
        var orResults = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and (Color = 'green' or Color = 'blue')")
            .ToListAsync();

        var inIds = inResults.Select(r => r.Id).OrderBy(x => x).ToList();
        var orIds = orResults.Select(r => r.Id).OrderBy(x => x).ToList();
        Assert.Equal(orIds, inIds);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task InString_MixedCase_EntryScan_AppliesAnalyzer()
    {
        // Regression guard: the residual IN value set must be analyzer-encoded with the FIELD's
        // analyzer so it matches the entry's stored (analyzed) term. With mixed-case values the
        // default analyzer lowercases both sides; if the IN value is built without the field
        // analyzer it stays "Bravo" while the stored term is "bravo" and the match is lost.
        // (Lowercase-only data masks this because lowercasing a lowercase value is a no-op.)
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            // 1 selective seed doc; its Name matches only via the SECOND IN term.
            await session.StoreAsync(new Item { Seed = "rare", Name = "Bravo" });

            // Filler bloats the IN posting list (Name='Alpha') so entry scan wins the cost check.
            for (int i = 0; i < FillerCount; i++)
                await session.StoreAsync(new Item { Seed = $"common-{i % 50}", Name = "Alpha" });

            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using var read = store.OpenAsyncSession();
        var results = await read.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Name in ('Alpha', 'Bravo')")
            .ToListAsync();

        Assert.Equal(1, results.Count);
        Assert.Equal("Bravo", results[0].Name);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NotInString_EntryScan_ReturnsComplementIncludingNull()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs Colors: i0=null,i1=green,i2=null,i3=red,i4=null,i5=blue,i6=null,i7=green,i8=null,i9=red
        // NOT IN ('green','blue') over rare docs -> exclude i1,i7 (green) and i5 (blue) =>
        // i0,i2,i4,i6,i8 (null) + i3,i9 (red) = 7. The null-field docs MUST appear (a doc lacking
        // the value satisfies NOT IN, matching the bitmap AndNot complement).
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and not (Color in ('green', 'blue'))")
            .ToListAsync();

        Assert.Equal(7, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.True(r.Color is null or "red");
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NotInLong_EntryScan_ReturnsComplement()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs Code = i (0..9). NOT IN [2,5,7,500] -> 0,1,3,4,6,8,9 => 7.
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and not (Code in (2, 5, 7, 500))")
            .ToListAsync();

        Assert.Equal(7, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.DoesNotContain(r.Code, new long[] { 2, 5, 7 });
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NotInDouble_EntryScan_ReturnsComplement()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs Score = i*1.5. NOT IN [3.0,7.5,12.0,999.5] -> exclude i2(3.0),i5(7.5),i8(12.0) => 7.
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and not (Score in (3.0, 7.5, 12.0, 999.5))")
            .ToListAsync();

        Assert.Equal(7, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.DoesNotContain(r.Score, new[] { 3.0, 7.5, 12.0 });
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NotAllIn_MultiValue_EntryScan_ReturnsComplement()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        // rare docs: even i -> Tags=[x,y,z] (contains {x,y} -> ALL IN true -> excluded);
        // odd i -> Tags=[x,z] (missing y -> ALL IN false -> included). odd i = 1,3,5,7,9 => 5.
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and not (Tags all in ('x', 'y'))")
            .ToListAsync();

        Assert.Equal(5, results.Count);
        Assert.All(results, r =>
        {
            Assert.Equal("rare", r.Seed);
            Assert.DoesNotContain("y", r.Tags);
        });
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NotIn_EntryScan_IsExactComplementOfIn_Parity()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var store = GetDocumentStore(options);
        await SeedAsync(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        var allRare = (await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare'").ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToList();

        var inIds = (await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and Color in ('green', 'blue')").ToListAsync())
            .Select(r => r.Id).ToHashSet();

        var notInIds = (await session.Advanced.AsyncRawQuery<Item>(
                "from Items where Seed = 'rare' and not (Color in ('green', 'blue'))").ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToList();

        // Within the seed, NOT IN must be exactly the complement of IN: (all rare) \ (rare ∩ IN).
        var expected = allRare.Where(id => inIds.Contains(id) == false).OrderBy(x => x).ToList();
        Assert.Equal(expected, notInIds);
    }

    private class Item
    {
        public string Id { get; set; }
        public string Seed { get; set; }
        public string Name { get; set; }
        public string Color { get; set; }
        public long Code { get; set; }
        public double Score { get; set; }
        public string[] Tags { get; set; }
    }
}
