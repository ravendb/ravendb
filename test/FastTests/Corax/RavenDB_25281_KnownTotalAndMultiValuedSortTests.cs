using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// RavenDB-25281 regression guards for two query-planner fixes:
///   - exists() known-total: a single non-boosted exists() reports its exact TotalResults from O(1) metadata
///     (entry count minus the field's non-existing posting list) instead of draining the posting set, so the
///     read stays page-bounded (EarlyExit) even under statistics.
///   - multi-valued sort guard: an equals/range clause on a MULTI-VALUED sort field must not drive a DirectScan.
///     SortedDrivingMatch walks every posting of a multi-valued field, so docs matching the term under one value
///     but a different value elsewhere leaked through unfiltered. The guard falls back to the bitmap pipeline +
///     SortingMatch, which applies the clause as a real filter.
/// </summary>
public class RavenDB_25281_KnownTotalAndMultiValuedSortTests : RavenTestBase
{
    public RavenDB_25281_KnownTotalAndMultiValuedSortTests(ITestOutputHelper output) : base(output)
    {
    }

    // Base doc has no Tagline property at all -> the index records it under the field's NON-EXISTING
    // posting list, so exists(Tagline) must exclude it.
    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class TaggedDoc : Doc
    {
        public string Tagline { get; set; }
    }

    private class Docs_ByTagline : AbstractIndexCreationTask<TaggedDoc>
    {
        public Docs_ByTagline()
        {
            // Map Name (present on every doc) AND Tagline: the base Doc entries are indexed via Name, but
            // lack Tagline entirely -> they land in Tagline's NON-EXISTING posting list, so exists(Tagline)
            // must exclude them. (Mapping Tagline alone would index the absent value as a null term = exists.)
            Map = docs => from d in docs
                select new { d.Name, d.Tagline };
        }
    }

    // exists(Tagline) reports the exact total from metadata and early-exits at the page even under
    // statistics (which would otherwise force a full count-draining scan).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_KnownTotal_ReportsExactTotalAndEarlyExitsWithStatistics(Options options)
    {
        const int withTag = 800;
        const int withoutTag = 400;

        options.ModifyDocumentStore = s => s.Conventions = new DocumentConventions { FindCollectionName = _ => "Docs" };
        using var store = GetDocumentStore(options);
        var index = new Docs_ByTagline();
        index.Execute(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < withTag; i++)
                await bulk.StoreAsync(new TaggedDoc { Name = $"name/{i}", Tagline = $"line {i}" }, $"docs/tag/{i}");
            for (int i = 0; i < withoutTag; i++)
                await bulk.StoreAsync(new Doc { Name = $"name/{i}" }, $"docs/plain/{i}");
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // Ground truth: the actual matching rows come from the bitmap, independent of the known-total that
        // only sources TotalResults. If the metadata count over/under-reports, it will diverge from this.
        var allMatches = await session.Advanced
            .AsyncRawQuery<TaggedDoc>($"from index '{index.IndexName}' where exists(Tagline)")
            .ToListAsync();
        int actualExists = allMatches.Count;
        Assert.True(actualExists > 25, $"Test needs more than a page of matches, but only {actualExists} exist.");

        var results = await session.Advanced
            .AsyncRawQuery<TaggedDoc>($"from index '{index.IndexName}' where exists(Tagline) limit 25 include timings()")
            .Statistics(out var stats)
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, results.Count);
        // The metadata-resolved total must match the real matching-document count exactly.
        Assert.Equal(actualExists, (int)stats.TotalResults);
        // And the data setup must actually exercise the "minus non-existing" arithmetic (some docs lack Tagline).
        Assert.Equal(withTag, actualExists);

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node. Plan: " + Describe(plan));
        // The read must NOT have drained the full posting set: the known total let the bitmap pipeline keep
        // its page limit even under statistics, so it stopped at the page (EarlyExit).
        Assert.True(compiled.Parameters.TryGetValue("EarlyExit", out var earlyExit) && earlyExit == "true",
            "Expected EarlyExit=true (known total skips the count drain), but plan was: " + Describe(plan) +
            " params: " + string.Join(", ", compiled.Parameters.Select(kv => kv.Key + "=" + kv.Value)));
    }

    private class Movie
    {
        public string Id { get; set; }
        public string[] Genres { get; set; }
        public int Seq { get; set; }
    }

    private class Movies_ByGenres : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByGenres()
        {
            Map = movies => from m in movies
                select new { m.Genres, m.Seq };
        }
    }

    // Deterministic seed: every movie has Drama plus one rotating extra genre, so Genres is multi-valued
    // (some docs hold 2 terms) and "Drama" is non-selective. This is exactly the shape that historically
    // drove an (incorrect) DirectScan on the multi-valued sort field.
    private static List<Movie> BuildMovies(int count)
    {
        string[] extra = { "Action", "Comedy", "Horror", "SciFi" };
        var movies = new List<Movie>(count);
        for (int i = 0; i < count; i++)
        {
            // 1 in 4 movies is Drama-only; the rest are Drama + one extra (multi-valued).
            var genres = i % 4 == 0
                ? new[] { "Drama" }
                : new[] { "Drama", extra[i % extra.Length] };
            movies.Add(new Movie { Id = $"movies/{i}", Genres = genres, Seq = i });
        }

        return movies;
    }

    // A Genres='Drama' filter that also drives ORDER BY Genres must return ONLY docs containing "Drama".
    // Cross-engine: Lucene has no DirectScan, so a match proves the Corax fallback is semantics-preserving
    // (the bug leaked non-matching documents).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MultiValuedSortField_EqualsDriven_ReturnsOnlyMatchingDocs(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Movies_ByGenres();
        index.Execute(store);
        var movies = BuildMovies(800);
        using (var bulk = store.BulkInsert())
        {
            foreach (var m in movies)
                await bulk.StoreAsync(m, m.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Movie>($"from index '{index.IndexName}' where Genres = 'Drama' order by Genres limit 25")
            .ToListAsync();

        Assert.Equal(25, results.Count);
        foreach (var r in results)
        {
            Assert.True(r.Genres != null && r.Genres.Any(g => string.Equals(g, "Drama", StringComparison.OrdinalIgnoreCase)),
                $"Document {r.Id} was returned but its Genres [{string.Join(", ", r.Genres ?? Array.Empty<string>())}] do not contain 'Drama'.");
        }
    }

    // Plan guard: the same query must NOT pick FieldSortedScan (DirectScan) on Corax — the multi-valued
    // sort field forces the bitmap pipeline + SortingMatch fallback.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task MultiValuedSortField_EqualsDriven_DoesNotUseDirectScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Movies_ByGenres();
        index.Execute(store);
        var movies = BuildMovies(800);
        using (var bulk = store.BulkInsert())
        {
            foreach (var m in movies)
                await bulk.StoreAsync(m, m.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Movie>($"from index '{index.IndexName}' where Genres = 'Drama' order by Genres limit 25 include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.NotEmpty(results);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        plan.Parameters.TryGetValue("OptimizationHint", out var hint);
        Assert.True(hint != "FieldSortedScan",
            "A multi-valued sort field must not drive a DirectScan, but OptimizationHint was 'FieldSortedScan'. Plan: " + Describe(plan));
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled?.Children?.FirstOrDefault(c => c.Operation == "DirectScan") == null,
            "Expected NO DirectScan node for a multi-valued sort field. Plan: " + Describe(plan));
    }

    // A query shaped `where f1 = $x order by f2` over a compound(f1, f2) field has no WHERE clause on the sort
    // field f2, yet the compound tree already stores f1's entries in f2 order. DirectScanCandidate is set for
    // this shape so the planner walks the compound subtree in f2 order and skips the SortingMatch heap.
    private class Film
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public int Year { get; set; }
    }

    private class Films_ByCategoryAndYear : AbstractIndexCreationTask<Film>
    {
        public Films_ByCategoryAndYear()
        {
            Map = films => from f in films
                select new { f.Category, f.Year };
            CompoundField("Category", "Year");
        }
    }

    // Single-valued Category (equality driver) and Year (sort key), cycled so neither is pre-sorted by
    // insertion order. 1 in 3 films is "Action" -> well above a 25-row page and below the scan cost cap.
    private static List<Film> BuildFilms(int count)
    {
        string[] categories = { "Action", "Comedy", "Drama" };
        var films = new List<Film>(count);
        for (int i = 0; i < count; i++)
            films.Add(new Film { Id = $"films/{i}", Category = categories[i % categories.Length], Year = 1980 + (i * 7) % 45 });

        return films;
    }

    // Plan guard: equality on the compound leading key + ORDER BY the second key (no filter on the sort field)
    // must drive the compound tree walk (CompoundSortedScan / DirectScan), NOT bitmap pipeline + SortingMatch.
    // Also checks the rows are correct, in ascending sort order.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task EqualityDrivenCompoundSort_UsesCompoundSortedScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in BuildFilms(300))
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // No filter on the sort field: equality on the compound leading key + ORDER BY the compound second key.
        // Engages DirectScanCandidate (the no-filter optimization) and must walk the compound tree in Year order.
        await AssertCompoundSortedScanInOrder(session, index.IndexName,
            $"from index '{index.IndexName}' where Category = 'Action' order by Year as long limit 25 include timings()");

        // Range on the sort field (the existing composite-range path): must ALSO come back in Year order, not
        // entry-id order. This is the shape that exposed the missing SortedDrivingMatch wrapper.
        await AssertCompoundSortedScanInOrder(session, index.IndexName,
            $"from index '{index.IndexName}' where Category = 'Action' and Year > 1990 order by Year as long limit 25 include timings()");
    }

    private async Task AssertCompoundSortedScanInOrder(IAsyncDocumentSession session, string indexName, string rql)
    {
        var results = await session.Advanced
            .AsyncRawQuery<Film>(rql)
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, results.Count);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node. Plan: " + Describe(plan));
        compiled.Parameters.TryGetValue("OptimizationHint", out var hint);
        Assert.True(hint == "CompoundSortedScan",
            "Expected the compound tree walk to drive equality+ORDER BY on compound(Category, Year), but " +
            "OptimizationHint was '" + hint + "' for [" + rql + "]. Plan: " + Describe(plan) + " params: " +
            string.Join(", ", compiled.Parameters.Select(kv => kv.Key + "=" + kv.Value)));
        Assert.True(compiled.Children?.FirstOrDefault(c => c.Operation == "DirectScan") != null,
            "Expected a DirectScan node (compound sorted walk) for [" + rql + "]. Plan: " + Describe(plan));

        int prev = int.MinValue;
        foreach (var r in results)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year >= prev, $"Results are not ascending by Year for [{rql}]: saw {prev} then {r.Year}.");
            prev = r.Year;
        }
    }

    // Descending plan guard: equality on the compound leading key + ORDER BY the second key DESC with no filter
    // on the sort field must stream via the compound tree walk DESCENDING (CompoundSortedScan, TreeDirection=
    // Backward), not the bitmap pipeline. The backward StartsWith provider seeks to successor(prefix) (end of the
    // field1 block) and walks down. Verifies plan, descending order, and paging.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DescendingEqualityDrivenCompoundSort_UsesBackwardCompoundSortedScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in BuildFilms(300))
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        var page1 = await session.Advanced
            .AsyncRawQuery<Film>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long desc limit 25 include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, page1.Count);

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node. Plan: " + Describe(plan));
        compiled.Parameters.TryGetValue("OptimizationHint", out var hint);
        Assert.True(hint == "CompoundSortedScan",
            "Descending equality+ORDER BY on compound(Category, Year) must stream via the compound tree walk, but " +
            "OptimizationHint was '" + hint + "'. Plan: " + Describe(plan));
        var directScan = FindOperation(plan, "DirectScan");
        Assert.True(directScan != null, "Expected a DirectScan node (compound sorted walk). Plan: " + Describe(plan));
        directScan.Parameters.TryGetValue("TreeDirection", out var direction);
        Assert.True(direction == "Backward",
            "Expected the compound scan to walk Backward for a descending ORDER BY, but TreeDirection was '" + direction +
            "'. Params: " + string.Join(", ", directScan.Parameters.Select(kv => kv.Key + "=" + kv.Value)));

        // Page 1 is descending by Year and all rows are the driving 'Action' value.
        int prev = int.MaxValue;
        foreach (var r in page1)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year <= prev, $"Results are not descending by Year: saw {prev} then {r.Year}.");
            prev = r.Year;
        }

        // Paging: the second page continues the same descending stream (no overlap/gap at the boundary).
        var page2 = await session.Advanced
            .AsyncRawQuery<Film>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long desc limit 25, 25")
            .ToListAsync();

        Assert.Equal(25, page2.Count);
        Assert.True(page2[0].Year <= page1[^1].Year,
            $"Page 2 must continue the descending order: page1 ended at {page1[^1].Year}, page2 started at {page2[0].Year}.");
        foreach (var r in page2)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year <= prev, $"Results are not descending across pages by Year: saw {prev} then {r.Year}.");
            prev = r.Year;
        }

        // The descending stream must start at the largest Year among the 'Action' films.
        var maxActionYear = BuildFilms(300).Where(f => f.Category == "Action").Max(f => f.Year);
        Assert.Equal(maxActionYear, page1[0].Year);
    }

    // Known-total / early-exit: the bare compound shape (equality on field1, ORDER BY field2, no field2 filter)
    // must resolve TotalResults from the driving term's cardinality and stop at the page, not drain the whole
    // driving set to count it, even under statistics.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task EqualityDrivenCompoundSort_ResolvesKnownTotalAndEarlyExitsUnderStatistics(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        var films = BuildFilms(300);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in films)
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);
        int actionCount = films.Count(f => f.Category == "Action");
        Assert.True(actionCount > 25, "Test needs more than a page of Action films.");

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Film>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long limit 25 include timings()")
            .Statistics(out var stats)
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, results.Count);
        // The exact total is reported from the term cardinality, not from draining the scan.
        Assert.Equal(actionCount, (int)stats.TotalResults);

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var directScan = FindOperation(plan, "DirectScan");
        Assert.True(directScan != null, "Expected a DirectScan node. Plan: " + Describe(plan));

        // Known total resolved up front (equals the driving 'Action' cardinality).
        Assert.True(directScan.Parameters.TryGetValue("KnownExactTotal", out var knownTotal),
            "Expected KnownExactTotal on the DirectScan. Params: " + string.Join(", ", directScan.Parameters.Select(kv => kv.Key + "=" + kv.Value)));
        Assert.Equal(actionCount, ParseCount(knownTotal));

        // The scan stopped at the page instead of draining the whole 'Action' set.
        directScan.Parameters.TryGetValue("StoppedAt", out var stoppedAt);
        Assert.True(stoppedAt != null && stoppedAt != "TreeExhausted",
            "Expected the scan to stop at the page (not TreeExhausted), but StoppedAt=" + (stoppedAt ?? "<null>"));
        Assert.True(directScan.Parameters.TryGetValue("TreeEntriesScanned", out var scanned) && ParseCount(scanned) < actionCount,
            "Expected fewer than the whole 'Action' set to be scanned, but TreeEntriesScanned=" + (scanned ?? "<null>") + " of " + actionCount);
    }

    private static int ParseCount(string n) => int.Parse(n, System.Globalization.NumberStyles.AllowThousands, System.Globalization.CultureInfo.InvariantCulture);

    // A single-field ORDER BY served by a CompoundSortedScan already emits in order, so the sort is elided into
    // the scan regardless of an $rvn_corax_sort hint. Pinning IndexOrderStreaming must not de-elide it (which
    // would wrap the scan in a SortingMatch that drains and re-sorts already-ordered output). On a sorted scan
    // the hint is a no-op; it only matters where a real SortingMatch exists (bitmap pipeline).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task EqualityDrivenCompoundSort_WithIndexOrderStreamingHint_StillElidesAndEarlyExits(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        var films = BuildFilms(300);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in films)
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);
        int actionCount = films.Count(f => f.Category == "Action");
        Assert.True(actionCount > 25, "Test needs more than a page of Action films.");

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Film>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long limit 25 include timings()")
            .AddParameter("rvn_corax_sort", "IndexOrderStreaming")
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, results.Count);
        int prev = int.MinValue;
        foreach (var r in results)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year >= prev, $"Results are not ascending by Year: saw {prev} then {r.Year}.");
            prev = r.Year;
        }

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);

        var compiled = FindOperation(plan, "CompiledQuery");
        compiled.Parameters.TryGetValue("OptimizationHint", out var hint);
        Assert.True(hint == "CompoundSortedScan",
            "Expected CompoundSortedScan even with the IndexOrderStreaming hint, but OptimizationHint was '" + hint + "'. Plan: " + Describe(plan));

        // The sort must be elided into the scan: no SortingMatch wrapper re-sorting the already-ordered output.
        Assert.True(FindOperation(plan, "SortingMatch") == null,
            "The IndexOrderStreaming hint de-elided the sort: a SortingMatch is wrapping the compound sorted scan. Plan: " + Describe(plan));

        // And the scan must stop at the page, not drain the whole 'Action' set.
        var directScan = FindOperation(plan, "DirectScan");
        Assert.True(directScan != null, "Expected a DirectScan node. Plan: " + Describe(plan));
        directScan.Parameters.TryGetValue("StoppedAt", out var stoppedAt);
        Assert.True(stoppedAt != null && stoppedAt != "TreeExhausted",
            "Expected the scan to stop at the page (not TreeExhausted), but StoppedAt=" + (stoppedAt ?? "<null>") + ". Plan: " + Describe(plan));
        Assert.True(directScan.Parameters.TryGetValue("TreeEntriesScanned", out var scanned) && ParseCount(scanned) < actionCount,
            "Expected fewer than the whole 'Action' set to be scanned, but TreeEntriesScanned=" + (scanned ?? "<null>") + " of " + actionCount);
    }

    // Cross-engine: the same shape must return exactly the matching rows in sort order on both engines.
    // Lucene has no compound/DirectScan, so a match proves the Corax compound-scan path is semantics-preserving.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task EqualityDrivenCompoundSort_ReturnsCorrectlyOrderedMatches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        var films = BuildFilms(300);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in films)
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);

        int expectedActionCount = films.Count(f => f.Category == "Action");

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Film>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long")
            .ToListAsync();

        Assert.Equal(expectedActionCount, results.Count);
        int prev = int.MinValue;
        foreach (var r in results)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year >= prev, $"Results are not ascending by Year: saw {prev} then {r.Year}.");
            prev = r.Year;
        }
    }

    private class FilmNullable
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public int? Year { get; set; }
    }

    private class FilmsNullable_ByCategoryAndYear : AbstractIndexCreationTask<FilmNullable>
    {
        public FilmsNullable_ByCategoryAndYear()
        {
            Map = films => from f in films
                select new { f.Category, f.Year };
            CompoundField("Category", "Year");
        }
    }

    // Every null-Year film is an "Action" film (i % 9 == 0 is a subset of i % 3 == 0), so the "Action"
    // result set mixes ~1/3 null years with real years — exactly the case where the compound scan's
    // null handling matters.
    private static List<FilmNullable> BuildFilmsWithNulls(int count)
    {
        string[] categories = { "Action", "Comedy", "Drama" };
        var films = new List<FilmNullable>(count);
        for (int i = 0; i < count; i++)
        {
            int? year = i % 9 == 0 ? null : 1980 + (i * 7) % 45;
            films.Add(new FilmNullable { Id = $"films/{i}", Category = categories[i % categories.Length], Year = year });
        }

        return films;
    }

    // Null behavior: `where Category = 'Action' order by Year` with some Action docs having a NULL sort value.
    // The compound walk would emit nulls at the wrong end (its null marker sorts after the real values),
    // contradicting NullsSortMode, so the bare shape with null sort values must fall back to the bitmap pipeline.
    // Pins (a) Corax matches Lucene's ordered sequence exactly, and (b) the plan fell back (OptimizationHint=BitmapPipeline).
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task EqualityDrivenCompoundSort_WithNullSortValues_FallsBackAndMatchesLucene()
    {
        var films = BuildFilmsWithNulls(300);

        var (luceneOrder, _) = await RunOrdered(RavenSearchEngineMode.Lucene);
        var (coraxOrder, coraxHint) = await RunOrdered(RavenSearchEngineMode.Corax);

        Assert.NotEmpty(coraxOrder);
        Assert.Contains((int?)null, coraxOrder); // the data must actually exercise null sort values

        string Shape(List<int?> o) =>
            $"count={o.Count}, nulls={o.Count(x => x == null)}, " +
            $"firstNullAt={o.FindIndex(x => x == null)}, lastNullAt={o.FindLastIndex(x => x == null)}, " +
            $"nonNullAscending={IsAscending(o.Where(x => x != null).Select(x => x.Value))}";

        Assert.True(luceneOrder.SequenceEqual(coraxOrder),
            $"Corax order diverges from Lucene.\n  Lucene: {Shape(luceneOrder)}\n  Corax:  {Shape(coraxOrder)}");

        // The guard must have demoted the bare compound shape to the bitmap pipeline (the only path that places
        // nulls per NullsSortMode). If this ever reports CompoundSortedScan, the null placement above is luck.
        Assert.Equal("BitmapPipeline", coraxHint);

        static bool IsAscending(IEnumerable<int> xs)
        {
            int prev = int.MinValue;
            foreach (var x in xs) { if (x < prev) return false; prev = x; }
            return true;
        }

        async Task<(List<int?> order, string hint)> RunOrdered(RavenSearchEngineMode mode)
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(mode));
            var index = new FilmsNullable_ByCategoryAndYear();
            await index.ExecuteAsync(store);
            using (var bulk = store.BulkInsert())
            {
                foreach (var f in films)
                    await bulk.StoreAsync(f, f.Id);
            }

            Indexes.WaitForIndexing(store);

            using var session = store.OpenAsyncSession();
            var results = await session.Advanced
                .AsyncRawQuery<FilmNullable>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long include timings()")
                .Timings(out var timings)
                .ToListAsync();

            string hint = null;
            if (timings.QueryPlan is QueryInspectionNode plan && FindOperation(plan, "CompiledQuery") is { } compiled)
                compiled.Parameters.TryGetValue("OptimizationHint", out hint);

            return (results.Select(r => r.Year).ToList(), hint);
        }
    }

    private class TieDoc
    {
        public string Id { get; set; }
        public long P { get; set; }
        public long S { get; set; }
    }

    private class TieDocs_Index : AbstractIndexCreationTask<TieDoc>
    {
        public TieDocs_Index()
        {
            Map = docs => from d in docs
                select new { d.P, d.S };
        }
    }

    // Two-field sort tie-break: ORDER BY P desc, S desc on a large single primary group must apply the secondary
    // (S) ordering. The tie-break path truncates to the top-`take` by S when the group exceeds the page cap, so a
    // 2000-doc group with a 25-row page must keep the 25 LARGEST S, not the smallest. Cross-engine pins the answer.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task TwoFieldSort_LargePrimaryGroup_AppliesSecondaryTieBreakDescending(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new TieDocs_Index();
        index.Execute(store);

        // Mirror the Movies/Showcase shape that exposed the bug: a huge primary group whose secondary is
        // mostly a common small value (0/1), with a few RARE large values scattered through the group. A
        // correct top-K truncation must surface those rare large values; a broken one keeps the common small ones.
        const int n = 40000;
        var bigS = new Dictionary<int, long>();
        for (int k = 0; k < 30; k++)
            bigS[k * 1300] = 1000 - k; // 30 docs with distinct large S (1000..971), scattered across the group
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < n; i++)
            {
                long s = bigS.TryGetValue(i, out var big) ? big : i % 2; // everyone else is 0 or 1
                await bulk.StoreAsync(new TieDoc { P = 1, S = s }, $"tie/{i}");
            }
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<TieDoc>($"from index '{index.IndexName}' order by P as long desc, S as long desc limit 25")
            .ToListAsync();

        Assert.Equal(25, results.Count);
        // P is constant, so this is purely S descending: the 25 largest S = 1000, 999, ..., 976.
        var actual = results.Select(r => r.S).ToList();
        var expected = Enumerable.Range(0, 25).Select(i => (long)(1000 - i)).ToList();
        Assert.Equal(expected, actual);
    }

    private class CatDoc
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public int UnitsInStock { get; set; }
    }

    private class CatDocs_ByCategoryUnits : AbstractIndexCreationTask<CatDoc>
    {
        public CatDocs_ByCategoryUnits()
        {
            Map = docs => from d in docs
                select new { d.Category, d.UnitsInStock };
            CompoundField("Category", "UnitsInStock");
        }
    }

    // Descending compound-prefix scan: `where Category = X order by Category, UnitsInStock desc` collapses to a
    // single DESCENDING sort over compound(Category, UnitsInStock). The compound walk must run the prefix BACKWARD
    // (a null seek term to the backward StartsWith provider crashed with NullReferenceException). Must return the
    // matching docs in descending UnitsInStock order.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DescendingCompoundSort_DoesNotCrashAndOrdersCorrectly(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new CatDocs_ByCategoryUnits();
        index.Execute(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                await bulk.StoreAsync(new CatDoc { Category = "categories/1-A", UnitsInStock = i }, $"cat/{i}");
            for (int i = 0; i < 50; i++)
                await bulk.StoreAsync(new CatDoc { Category = "categories/2-A", UnitsInStock = 1000 + i }, $"other/{i}");
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<CatDoc>($"from index '{index.IndexName}' where Category = 'categories/1-A' order by Category, UnitsInStock as long desc limit 25")
            .ToListAsync();

        Assert.Equal(25, results.Count);
        int prev = int.MaxValue;
        foreach (var r in results)
        {
            Assert.Equal("categories/1-A", r.Category);
            Assert.True(r.UnitsInStock <= prev, $"Results not descending by UnitsInStock: {prev} then {r.UnitsInStock}");
            prev = r.UnitsInStock;
        }
        // The largest UnitsInStock among the 200 'categories/1-A' docs is 199.
        Assert.Equal(199, results[0].UnitsInStock);
    }

    // RavenDB-26831: a compound numeric member must sort negatives correctly. New indexes (built at
    // CoraxOrderPreservingCompoundNumericEncoding or higher) encode signed longs order-preserving, so the
    // CompoundSortedScan walk/range over a field with negative values is correct. Cross-engine pins it to Lucene.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CompoundSort_NegativeNumericValues_OrderedCorrectly(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new CatDocs_ByCategoryUnits();
        index.Execute(store);

        var values = new[] { -50, -16, -1, 0, 1, 7, 100 };
        using (var bulk = store.BulkInsert())
        {
            int i = 0;
            foreach (var u in values)
                await bulk.StoreAsync(new CatDoc { Category = "categories/1-A", UnitsInStock = u }, $"cat/{i++}");
        }

        Indexes.WaitForIndexing(store);
        using var session = store.OpenAsyncSession();

        // Ascending: this pins Category and walks compound(Category, UnitsInStock) ascending — exercises the
        // order-preserving encoding directly. Negatives must come first.
        var asc = await session.Advanced
            .AsyncRawQuery<CatDoc>($"from index '{index.IndexName}' where Category = 'categories/1-A' order by Category, UnitsInStock as long include timings()")
            .Timings(out var ascTimings)
            .ToListAsync();
        Assert.Equal(new[] { -50, -16, -1, 0, 1, 7, 100 }, asc.Select(r => r.UnitsInStock).ToList());

        if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
        {
            // Make sure we actually exercised the compound walk (not a plain SortingMatch that would pass regardless).
            var plan = ascTimings.QueryPlan as QueryInspectionNode;
            var compiled = FindOperation(plan, "CompiledQuery");
            compiled?.Parameters.TryGetValue("OptimizationHint", out var hint);
            Assert.True(compiled?.Parameters.GetValueOrDefault("OptimizationHint") == "CompoundSortedScan",
                "Expected CompoundSortedScan to exercise the compound numeric encoding. Plan: " + Describe(plan));
        }

        // Descending.
        var desc = await session.Advanced
            .AsyncRawQuery<CatDoc>($"from index '{index.IndexName}' where Category = 'categories/1-A' order by Category, UnitsInStock as long desc")
            .ToListAsync();
        Assert.Equal(new[] { 100, 7, 1, 0, -1, -16, -50 }, desc.Select(r => r.UnitsInStock).ToList());

        // Range crossing zero: > -10 must include -1,0,1,… and exclude -16,-50 — both the rows AND the count.
        var range = await session.Advanced
            .AsyncRawQuery<CatDoc>($"from index '{index.IndexName}' where Category = 'categories/1-A' and UnitsInStock > -10 order by Category, UnitsInStock as long")
            .Statistics(out var rangeStats)
            .ToListAsync();
        Assert.Equal(new[] { -1, 0, 1, 7, 100 }, range.Select(r => r.UnitsInStock).ToList());
        Assert.Equal(5, (int)rangeStats.TotalResults);
    }

    // Descending sort over ONLY the second compound member (leading member pinned by WHERE but not in the ORDER
    // BY): `where Category = X order by UnitsInStock desc`. The compound walk's `forward` is false with no field2
    // range — the path that built a backward StartsWith provider with a null seek term and crashed. Must not
    // crash and must return descending UnitsInStock.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DescendingSortOnSecondCompoundMemberOnly_DoesNotCrashAndOrdersCorrectly(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new CatDocs_ByCategoryUnits();
        index.Execute(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 200; i++)
                await bulk.StoreAsync(new CatDoc { Category = "categories/1-A", UnitsInStock = i }, $"cat/{i}");
            for (int i = 0; i < 50; i++)
                await bulk.StoreAsync(new CatDoc { Category = "categories/2-A", UnitsInStock = 1000 + i }, $"other/{i}");
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<CatDoc>($"from index '{index.IndexName}' where Category = 'categories/1-A' order by UnitsInStock as long desc limit 25")
            .ToListAsync();

        Assert.Equal(25, results.Count);
        int prev = int.MaxValue;
        foreach (var r in results)
        {
            Assert.Equal("categories/1-A", r.Category);
            Assert.True(r.UnitsInStock <= prev, $"Results not descending by UnitsInStock: {prev} then {r.UnitsInStock}");
            prev = r.UnitsInStock;
        }
        Assert.Equal(199, results[0].UnitsInStock);
    }

    private static QueryInspectionNode FindOperation(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindOperation(child, operation);
            if (found != null)
                return found;
        }

        return null;
    }

    private static string Describe(QueryInspectionNode node, int depth = 0)
    {
        if (node == null)
            return "<null>";
        var prefix = new string(' ', depth * 2);
        var line = prefix + node.Operation;
        if (node.Children == null || node.Children.Count == 0)
            return line;
        return line + Environment.NewLine + string.Join(Environment.NewLine, node.Children.Select(c => Describe(c, depth + 1)));
    }
}
