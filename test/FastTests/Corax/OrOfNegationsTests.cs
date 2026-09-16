using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Validates the De Morgan fold for OR chains whose members are ALL negations
/// (<c>¬A ∨ ¬B = ¬(A ∧ B)</c>): the planner intersects the positive forms once and takes a
/// single complement instead of one <c>FillAllEntries + AndNot</c> per member. The fold must be
/// result-identical to the un-folded plan, so every test compares the engine's answer against a
/// brute-force expectation computed over the seeded set. Cross-engine (Corax vs Lucene) parity is
/// also asserted via <see cref="RavenSearchEngineMode.All"/> — Lucene never folds, so a match proves
/// the rewrite is semantics-preserving, including null / missing-field handling.
/// </summary>
public class OrOfNegationsTests : RavenTestBase
{
    public OrOfNegationsTests(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Color { get; set; }
        public double Score { get; set; }
        public long Code { get; set; }
    }

    // Deterministic seed: Color cycles green/blue/red/(missing), Score == Code == index.
    private static List<Item> BuildSeed(int count)
    {
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            string color = (i % 4) switch { 0 => "green", 1 => "blue", 2 => "red", _ => null };
            items.Add(new Item { Id = $"items/{i}", Color = color, Score = i, Code = i });
        }

        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var session = store.OpenAsyncSession();
        foreach (var it in items)
            await session.StoreAsync(it, it.Id);
        await session.SaveChangesAsync();
    }

    private static async Task<List<string>> RunIds(IDocumentStore store, string query)
    {
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(query).ToListAsync();
        return results.Select(r => r.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();
    }

    private static List<string> Expected(IEnumerable<Item> items, Func<Item, bool> predicate) =>
        items.Where(predicate).Select(i => i.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();

    // ¬(Color ∈ {green,blue}) ∨ ¬(Score == 5)  ==  ¬(Color ∈ {green,blue} ∧ Score == 5).
    // Two negated members -> folds. Docs with missing Color satisfy the complement.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task NotInOrNotEquals_Folds_MatchesComplement(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Score != 5 or not (Color in ('green', 'blue'))");

        var expected = Expected(items,
            i => !((i.Color is "green" or "blue") && i.Score == 5));

        Assert.Equal(expected, actual);
        // Only items/5 is excluded: i=5 -> Color "blue" (5%4==1) and Score 5, the lone member of the intersection.
        Assert.DoesNotContain("items/5", actual);
        Assert.Equal(items.Count - 1, actual.Count);
        // Missing-Color docs (i%4==3) are present in the complement.
        Assert.Contains("items/3", actual);
    }

    // Three all-negated members: ¬A ∨ ¬B ∨ ¬C == ¬(A ∧ B ∧ C). Exercises the N-way intersect-once path.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task ThreeNegations_Folds_MatchesComplement(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Color != 'red' or Score != 6 or Code != 7");

        var expected = Expected(items,
            i => !((i.Color == "red") && i.Score == 6 && i.Code == 7));

        // No single doc has Color=red AND Score=6 AND Code=7 (Score==Code==index, so Score=6 implies Code=6),
        // so the intersection is empty and the complement is the whole set.
        Assert.Equal(expected, actual);
        Assert.Equal(items.Count, actual.Count);
    }

    // Mixed chain (one negated, one positive) must NOT fold and must still be correct:
    // ¬(Color ∈ {green}) ∨ (Score == 6). Positive member's matches still OR in.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MixedNegatedAndPositive_DoesNotFold_StillCorrect(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Score = 6 or not (Color in ('green'))");

        var expected = Expected(items,
            i => i.Color != "green" || i.Score == 6);

        Assert.Equal(expected, actual);
        // items/4 has Color=green (4%4==0) and Score 4 != 6 -> excluded.
        Assert.DoesNotContain("items/4", actual);
        // items/0 has Color=green but is rescued by nothing (Score 0 != 6) -> excluded too.
        Assert.DoesNotContain("items/0", actual);
        // items/8 Color=green Score 8 -> excluded; items/24 Color=green Score 24 -> excluded.
        // The green doc whose Score==6? none (green => i%4==0 => Score multiple of 4) so all green excluded.
        Assert.DoesNotContain("items/8", actual);
    }

    // Mixed chain with a positive PREFIX and a foldable negated SUFFIX of ≥2 members:
    // Score = 10 OR ¬(Color ∈ {red}) OR ¬(Code ∈ {6}). After the cardinality sort the positive sorts
    // first and the two negations form a contiguous suffix, which folds to ¬(Color=red ∧ Code=6) and
    // ORs back over the positive. Result == everything except the lone intersection member (item6),
    // which the positive Score=10 does not rescue.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task PositivePrefix_TwoNegationSuffix_FoldsSuffix(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Score = 10 or not (Color in ('red')) or not (Code in (6))");

        var expected = Expected(items,
            i => i.Score == 10 || i.Color != "red" || i.Code != 6);

        Assert.Equal(expected, actual);
        // items/6 is red (6%4==2) with Code==6 -> the lone intersection member; Score 6 != 10 so the
        // positive does not rescue it -> excluded.
        Assert.DoesNotContain("items/6", actual);
        Assert.Equal(items.Count - 1, actual.Count);
        // items/10 is in the complement anyway (Color "red"? 10%4==2 -> red, Code 10 != 6 -> not in
        // intersection) and is additionally the positive match.
        Assert.Contains("items/10", actual);
        // Missing-Color docs (i%4==3) land in the complement.
        Assert.Contains("items/3", actual);
    }

    // AND context: selective accumulator AND an all-negated OR sub-group.
    // Color = 'red' AND (Score != 6 OR Code != 6)  ==  Color='red' AND ¬(Score=6 ∧ Code=6)  ==  red \ {item6}.
    // The sub-group folds into the accumulator as acc \ (Score=6 ∧ Code=6) with no FillAllEntries.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task AndAccumulator_NegatedOrGroup_FoldsToAndNot(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Color = 'red' and (Score != 6 or Code != 6)");

        var expected = Expected(items,
            i => i.Color == "red" && !(i.Score == 6 && i.Code == 6));

        Assert.Equal(expected, actual);
        // items/6 is red (6%4==2) with Score==Code==6 -> the lone intersection member -> excluded.
        Assert.DoesNotContain("items/6", actual);
        // items/2 is red and not in the intersection -> present.
        Assert.Contains("items/2", actual);
    }

    // Three-member all-negated OR sub-group AND'd into an accumulator (N-member fold in AND context).
    // Color = 'red' AND (Score != 6 OR Code != 10 OR Color != 'red'): the intersection
    // (Score=6 ∧ Code=10 ∧ Color=red) is empty (Score==Code so 6≠10), so result == all red docs.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task AndAccumulator_ThreeNegatedOrGroup_Folds(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Color = 'red' and (Score != 6 or Code != 10 or Color != 'red')");

        var expected = Expected(items,
            i => i.Color == "red" && !(i.Score == 6 && i.Code == 10 && i.Color == "red"));

        Assert.Equal(expected, actual);
        // Every red doc survives (empty intersection): red docs are i%4==2.
        Assert.Contains("items/6", actual);
        Assert.Contains("items/2", actual);
    }
}
