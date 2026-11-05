using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

// Verifies that an OR of two boosted clauses sorted by score() comes back in the correct boost-weighted order:
//   where boost(search(Overview, $t), W1) or boost(Genres = $g, W2) order by score()
// The shape mirrors the production 'Movies/Showcase' query (search overview boosted x10 OR genres equality boosted x2).
// Assertions are on the resulting DOCUMENT ORDER (the sort output) rather than the reported @index-score, because the
// score surfaced in metadata is the base relevance, not the boosted value used for the sort.
public class BoostedScoreSorting : RavenTestBase
{
    public BoostedScoreSorting(ITestOutputHelper output) : base(output)
    {
    }

    private class Movie
    {
        public string Id { get; set; }
        public string Overview { get; set; }
        public string[] Genres { get; set; }
    }

    private class Movies_Showcase : AbstractIndexCreationTask<Movie>
    {
        public Movies_Showcase()
        {
            Map = movies => from m in movies select new { m.Overview, m.Genres };
            // search(Overview, ...) requires the field to be analyzed.
            Index(x => x.Overview, FieldIndexing.Search);
        }
    }

    // Faithful to the production query (search x10 OR genres x2). The documents that match BOTH clauses share the
    // EXACT overview text of the war-only documents, so their x10 search contribution is identical — and they get the
    // x2 genres contribution on top. Their combined score is therefore strictly greater than every single-clause
    // match, so they must sort to the top. Also checks that documents matching neither clause are excluded.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OrOfBoostedClauses_SortsCombinedMatchesFirst(Options options)
    {
        const int perGroup = 3;
        using var store = GetDocumentStore(options);
        new Movies_Showcase().Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 1; i <= perGroup; i++)
            {
                // matches both clauses: "war" in overview (x10) AND Drama genre (x2)
                s.Store(new Movie { Overview = "war torn nation", Genres = new[] { "Drama" } }, $"movies/both/{i}");
                // matches the search clause only (same overview as 'both' -> identical x10 contribution)
                s.Store(new Movie { Overview = "war torn nation", Genres = new[] { "Comedy" } }, $"movies/war/{i}");
                // matches the genres clause only (x2)
                s.Store(new Movie { Overview = "a quiet romance", Genres = new[] { "Drama" } }, $"movies/drama/{i}");
                // matches neither -> must not appear
                s.Store(new Movie { Overview = "a quiet romance", Genres = new[] { "Comedy" } }, $"movies/none/{i}");
            }

            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var s = store.OpenSession())
        {
            var results = s.Advanced
                .RawQuery<Movie>("from index 'Movies/Showcase' " +
                                 "where boost(search(Overview, $t), 10) or boost(Genres = $g, 2) " +
                                 "order by score() include timings() limit 25")
                .AddParameter("t", "war")
                .AddParameter("g", "Drama")
                .ToList();

            // 3 both + 3 war-only + 3 drama-only match; the 3 'none' documents are excluded.
            Assert.Equal(3 * perGroup, results.Count);
            Assert.DoesNotContain(results, r => r.Id.StartsWith("movies/none/"));

            // The 'both' documents (x10 search + x2 genres) strictly outscore both single-clause groups, so they take
            // the top {perGroup} positions (ties among themselves are fine — we compare the id set).
            var topIds = results.Take(perGroup).Select(r => r.Id).OrderBy(id => id).ToList();
            var expectedBothIds = Enumerable.Range(1, perGroup).Select(i => $"movies/both/{i}").ToList();
            Assert.Equal(expectedBothIds, topIds);
        }
    }

    // Proves the boost WEIGHT drives the sort (not just the base relevance). Uses two equality clauses on the SAME
    // field with equal cardinality, so their base scores are identical and the boost weight alone decides the order:
    // boost Drama higher -> Drama docs lead; flip the weights -> Action docs lead. If the boosts were ignored (or not
    // applied to the score) the relative group order could not flip.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void BoostWeight_DeterminesGroupOrder(Options options)
    {
        const int perGroup = 3;
        using var store = GetDocumentStore(options);
        new Movies_Showcase().Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 1; i <= perGroup; i++)
            {
                s.Store(new Movie { Overview = "n/a", Genres = new[] { "Drama" } }, $"movies/drama/{i}");
                s.Store(new Movie { Overview = "n/a", Genres = new[] { "Action" } }, $"movies/action/{i}");
            }

            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var s = store.OpenSession())
        {
            // Drama boosted higher -> Drama docs lead (equal base scores, so the x10 vs x2 weight decides).
            var dramaBoosted = s.Advanced
                .RawQuery<Movie>("from index 'Movies/Showcase' " +
                                 "where boost(Genres = $a, 10) or boost(Genres = $b, 2) " +
                                 "order by score() limit 25")
                .AddParameter("a", "Drama")
                .AddParameter("b", "Action")
                .ToList();
            AssertGroupLeads(dramaBoosted, leadingPrefix: "movies/drama/", trailingPrefix: "movies/action/", perGroup);

            // Flip the weights -> Action docs lead. The order changed purely because of the boost weights.
            var actionBoosted = s.Advanced
                .RawQuery<Movie>("from index 'Movies/Showcase' " +
                                 "where boost(Genres = $a, 2) or boost(Genres = $b, 10) " +
                                 "order by score() limit 25")
                .AddParameter("a", "Drama")
                .AddParameter("b", "Action")
                .ToList();
            AssertGroupLeads(actionBoosted, leadingPrefix: "movies/action/", trailingPrefix: "movies/drama/", perGroup);
        }
    }

    private static void AssertGroupLeads(List<Movie> results, string leadingPrefix, string trailingPrefix, int perGroup)
    {
        Assert.Equal(2 * perGroup, results.Count);

        // Every leading-group document must rank ahead of every trailing-group document.
        int lastLeading = results.FindLastIndex(r => r.Id.StartsWith(leadingPrefix));
        int firstTrailing = results.FindIndex(r => r.Id.StartsWith(trailingPrefix));
        Assert.True(lastLeading >= 0 && firstTrailing >= 0, "both groups must be present in the results");
        Assert.True(lastLeading < firstTrailing,
            $"Expected all '{leadingPrefix}' docs to rank ahead of all '{trailingPrefix}' docs. " +
            "Order: " + string.Join(", ", results.Select(r => r.Id)));
    }
}
