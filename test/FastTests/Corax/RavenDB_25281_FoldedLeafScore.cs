using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

// Regression for RavenDB-25281 (score-sorted OR of scored term leaves).
//
// The bitmap-pipeline plan folds each scored term leaf into the result accumulator with a zero-copy OR that
// STEALS the leaf's RoaringBitmap containers and marks the leaf consumed. The score pass (ScoreSorted ->
// ScorePresentSorted) then re-reads every leaf. The FIRST leaf folded into the empty accumulator has ALL its
// containers stolen, so at score time its bitmap is empty and it contributes ZERO to every doc's score. A doc
// whose only relevance signal is that first-folded clause therefore silently loses its entire score and drops
// to the bottom of the ranking. The fix clones scored leaves into the fold (gated on score-sorted queries) so
// the leaf survives intact for scoring.
//
// This test makes the first-folded clause (`search(Body, 'alpha')`) a RARE, high-IDF term matched by exactly
// one document. With correct scoring that document tops a score() sort. Under the bug its leaf is emptied first,
// its score collapses to ~0, and it sinks below the common-term documents -> the ranking flips. Asserting the
// rare-term document ranks FIRST is a relation that holds regardless of exact BM25 values.
public class RavenDB_25281_FoldedLeafScore : RavenTestBase
{
    public RavenDB_25281_FoldedLeafScore(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Body { get; set; }
    }

    private class Items_ByBody : AbstractIndexCreationTask<Item>
    {
        public Items_ByBody()
        {
            Map = items => from i in items select new { i.Body };
            Index(x => x.Body, FieldIndexing.Search);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void FirstFoldedScoredLeaf_KeepsScoreForRanking()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        new Items_ByBody().Execute(store);

        using (var s = store.OpenSession())
        {
            // 'alpha' is rare (one doc) -> high IDF -> this doc must top a score() sort when scored correctly.
            s.Store(new Item { Body = "alpha" }, "items/rare");
            // 'beta' is common (many docs) -> low IDF. These match only the second, non-first-folded clause.
            for (int i = 0; i < 12; i++)
                s.Store(new Item { Body = "beta" }, $"items/common/{i}");
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var s = store.OpenSession())
        {
            // 'alpha' is the first OR clause, so its leaf is folded first into the empty accumulator and is the
            // one the bug fully empties before scoring.
            var results = s.Advanced
                .RawQuery<Item>("from index 'Items/ByBody' " +
                                "where search(Body, $rare) or search(Body, $common) " +
                                "order by score() limit 50")
                .AddParameter("rare", "alpha")
                .AddParameter("common", "beta")
                .ToList();

            Assert.Equal(13, results.Count);

            var order = results.Select(r => r.Id).ToList();

            // The rare-term doc must rank first (highest IDF). Under the bug its first-folded leaf is emptied,
            // its score collapses to ~0, and it sinks to the bottom -> this assertion fails.
            Assert.Equal("items/rare", order[0]);
        }
    }

    // Fold-order-INDEPENDENT hardening for the same bug.
    //
    // The test above relies on RQL clause order mapping to fold order (first clause folded first -> its leaf is the
    // emptied one). A planner that reorders which leaf folds first could mask the bug without failing that test.
    //
    // This variant is immune to fold order. Three docs share the first id-block: docBoth matches BOTH terms
    // (Body = "alpha beta"), docAlpha matches only "alpha", docBeta matches only "beta". For a two-scored-leaf OR
    // sorted by score():
    //   * Correct (fix): score(docBoth) = contribution(alpha) + contribution(beta), strictly greater than each
    //     single-term doc, which carries only one contribution.
    //   * Bug (either fold order): whichever leaf folds first is emptied, so ONE term contributes 0 to every doc.
    //     docBoth then keeps only the surviving term's contribution and TIES the single-term doc that also matches
    //     that surviving term -> docBoth is no longer strictly greater than both single-term docs.
    // The strict-greater-than-BOTH relation therefore fails regardless of which leaf the planner folds first, so the
    // assertion is inherently fold-order-independent. We run it with both clause orders as belt-and-suspenders.
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void DocMatchingBothScoredLeaves_ScoresStrictlyAboveEitherSingleTerm()
    {
        AssertDocBothScoresAboveBothSingles(rareFirst: true);
        AssertDocBothScoresAboveBothSingles(rareFirst: false);
    }

    private void AssertDocBothScoresAboveBothSingles(bool rareFirst)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        new Items_ByBody().Execute(store);

        using (var s = store.OpenSession())
        {
            // All three land in the first id-block (tiny data set).
            s.Store(new Item { Body = "alpha beta" }, "items/both");
            s.Store(new Item { Body = "alpha" }, "items/alpha");
            s.Store(new Item { Body = "beta" }, "items/beta");
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var s = store.OpenSession())
        {
            // Two scored, non-consuming... no: bare `search` leaves take the consuming fold path (the one the bug
            // corrupts). `boost(...)` would wrap in a non-IBitmapQueryMatch and dodge the fold, so we must NOT use it.
            // Swap the clause order between runs so we exercise both fold orders.
            var where = rareFirst
                ? "where search(Body, $a) or search(Body, $b)"
                : "where search(Body, $b) or search(Body, $a)";

            var results = s.Advanced
                .RawQuery<Item>($"from index 'Items/ByBody' {where} order by score() limit 50")
                .AddParameter("a", "alpha")
                .AddParameter("b", "beta")
                .ToList();

            Assert.Equal(3, results.Count);

            double ScoreOf(string id)
            {
                var doc = results.Single(r => r.Id == id);
                var meta = s.Advanced.GetMetadataFor(doc);
                return (double)meta[Raven.Client.Constants.Documents.Metadata.IndexScore];
            }

            var scoreBoth = ScoreOf("items/both");
            var scoreAlpha = ScoreOf("items/alpha");
            var scoreBeta = ScoreOf("items/beta");

            // Strictly greater than BOTH single-term docs. Under the bug docBoth loses one term's contribution and
            // ties the surviving single-term doc, so one of these fails whichever leaf was folded (and emptied) first.
            Assert.True(scoreBoth > scoreAlpha,
                $"(rareFirst={rareFirst}) expected score(items/both)={scoreBoth} strictly > score(items/alpha)={scoreAlpha}");
            Assert.True(scoreBoth > scoreBeta,
                $"(rareFirst={rareFirst}) expected score(items/both)={scoreBoth} strictly > score(items/beta)={scoreBeta}");
        }
    }
}
