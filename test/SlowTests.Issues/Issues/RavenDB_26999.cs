using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26999(ITestOutputHelper output) : RavenTestBase(output)
{
    // Two things broke for terms backed by a posting list: the bitmap pipeline skipped TermMatch.Fill (the only place
    // that feeds BM25), and the score pass over a large posting list never decoded what it read. Bm25Relevance keeps a
    // term's frequencies in memory only below ~105k entries (MaximumDocumentCapacity) and re-reads the posting list
    // above that, so every dataset here has a term on each side of the line. The score buffer starts at
    // Bm25Relevance.InitialScoreValue (1/1_000_000f); a score equal to it means BM25 contributed nothing at all.
    private const double InitialScoreValue = 1 / 1_000_000f;

    private class Movie
    {
        public string Id { get; set; }
        public string Genre { get; set; }
        public string Status { get; set; }
        public string Title { get; set; }
    }

    private class ScoreOnly
    {
        public double Score { get; set; }
    }

    private class Movies_Showcase : AbstractIndexCreationTask<Movie>
    {
        public Movies_Showcase()
        {
            Map = movies => from m in movies
                            select new
                            {
                                m.Genre,
                                m.Status,
                                m.Title
                            };

            Index(x => x.Genre, FieldIndexing.Exact);
            Index(x => x.Title, FieldIndexing.Search);

            // Set per index, the way production indexes usually do it - not via database settings.
            Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = "true";
        }
    }

    private IDocumentStore SetupStore(RavenSearchEngineMode engine, int totalDocuments, int rareEvery)
    {
        var store = GetDocumentStore(Options.ForSearchEngine(engine));

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < totalDocuments; i++)
            {
                bulk.Store(new Movie
                {
                    // "Drama" is sparse (every rareEvery-th document) but still numerous - that combination is what
                    // pushes its posting list out of the small representation.
                    Genre = i % rareEvery == 0 ? "Drama" : "Filler",
                    Status = i % 3 == 0 ? "Released" : "Planned",
                    Title = i % rareEvery == 0 ? "the matrix reloaded" : "some other movie title"
                });
            }
        }

        store.ExecuteIndex(new Movies_Showcase());
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(15));

        return store;
    }

    private static double[] Scores(IDocumentStore store, string where, string orderBy = "order by score()")
    {
        using var session = store.OpenSession();
        return session.Advanced
            .RawQuery<ScoreOnly>($@"from index 'Movies/Showcase' as m
                                    where {where}
                                    {orderBy}
                                    select {{ Score: getMetadata(m)[""@index-score""] }}
                                    limit 5")
            .ToList()
            .Select(x => x.Score)
            .ToArray();
    }

    private static double TopScore(IDocumentStore store, string where)
    {
        var scores = Scores(store, where);
        Assert.NotEmpty(scores);
        return scores[0];
    }

    // RQL quirk, same on both engines: 'order by score()' is most relevant first, so 'desc' puts the least relevant first.
    private static double LowestScore(IDocumentStore store, string where)
    {
        var scores = Scores(store, where, "order by score() desc");
        Assert.NotEmpty(scores);
        return scores[0];
    }

    private void AssertScored(IDocumentStore store, string where, string what)
    {
        var scores = Scores(store, where);

        Assert.NotEmpty(scores);

        foreach (var score in scores)
            Assert.False(Math.Abs(score - InitialScoreValue) < 1e-12,
                $"{what}: score is the untouched InitialScoreValue ({score}) - BM25 contributed nothing");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [InlineData(100_000, 100)]    // Drama df = 1_000, Released df = 33_334 - both stored in memory at score time
    [InlineData(1_000_000, 100)]  // Drama df = 10_000, Released df = 333_334 - Released is re-read from the posting list at score time
    public void ScoreIsComputedRegardlessOfPostingListSize(int totalDocuments, int rareEvery)
    {
        using var store = SetupStore(RavenSearchEngineMode.Corax, totalDocuments, rareEvery);

        // Single terms first: they are the only way to pin one code path at a time. The compound shapes below pass as
        // soon as any one leaf scores, so they cover the primitives (OR, AND, AND NOT, IN) but not each leaf in them.
        AssertScored(store, "m.Genre = 'Drama'", "equals (OR fold)");
        AssertScored(store, "m.Status = 'Released'", "equals, term above the stored threshold in the 1M case");
        AssertScored(store, "search(m.Title, 'matrix')", "search on analyzed field");
        AssertScored(store, "m.Genre = 'Drama' and m.Status = 'Released'", "AND of two terms");
        AssertScored(store, "m.Genre = 'Drama' and m.Status != 'Planned'", "AND NOT");
        AssertScored(store, "m.Genre = 'Drama' or m.Status = 'Released'", "OR of two terms");
        AssertScored(store, "boost(m.Genre = 'Drama', 100)", "explicit boost");
        AssertScored(store, "m.Genre in ('Drama', 'Western')", "IN");

        // Every 'Released' document holds the term exactly once, so BM25 gives all of them the same score. The lowest
        // one is where a partially read posting list shows up: whatever the score pass never reached stays at 1e-6.
        Assert.Equal(TopScore(store, "m.Status = 'Released'"), LowestScore(store, "m.Status = 'Released'"), 6);

        // A negated term is subtracted, never scored (Lucene's MUST_NOT does the same): documents that reach the result
        // through 'Drama' score exactly as they do without the negated branch, whatever the size of 'Planned'.
        Assert.Equal(TopScore(store, "m.Genre = 'Drama'"), TopScore(store, "m.Genre = 'Drama' or m.Status != 'Planned'"), 6);
    }

    // Shared body, no attribute on purpose: two 1M-document stores belong in StressTests - see StressTests.Issues.RavenDB_26999.
    public void CoraxScoresMatchLuceneOrderOfMagnitude()
    {
        const int documents = 1_000_000;
        const int rareEvery = 100;
        string[] clauses = ["m.Genre = 'Drama'", "search(m.Title, 'matrix')", "m.Genre = 'Drama' and m.Status = 'Released'"];

        // One 1M-document store at a time - the two engines never need to coexist.
        var coraxScores = new Dictionary<string, double>();
        using (var corax = SetupStore(RavenSearchEngineMode.Corax, documents, rareEvery))
        {
            foreach (var where in clauses)
                coraxScores[where] = TopScore(corax, where);
        }

        using var lucene = SetupStore(RavenSearchEngineMode.Lucene, documents, rareEvery);
        foreach (var where in clauses)
        {
            var coraxScore = coraxScores[where];
            var luceneScore = TopScore(lucene, where);

            // The formulas differ, so the scores are not equal - but they must live in the same range instead of
            // Corax collapsing to 1e-6 while Lucene reports a healthy score.
            Assert.True(coraxScore > luceneScore / 100 && coraxScore < luceneScore * 100,
                $"{where}: corax={coraxScore} vs lucene={luceneScore} - orders of magnitude apart");
        }
    }

    // Shared body, no attribute on purpose: a 1M-document store belongs in StressTests - see StressTests.Issues.RavenDB_26999.
    public void ScoresStayOrderedByRelevanceAcrossPostingListSizes()
    {
        using var store = SetupStore(RavenSearchEngineMode.Corax, 1_000_000, 100);

        // Rarer term must score higher than the common one - the property that collapsed to a flat 1e-6 for every
        // term whose posting list was large. The common term is judged by its lowest score, so a score pass that
        // gives up partway through its posting list cannot hide behind the documents it did reach.
        var rare = TopScore(store, "m.Genre = 'Drama'");           // df = 10_000
        var common = LowestScore(store, "m.Status = 'Released'");  // df = 333_334

        Assert.True(rare > common, $"rare term scored {rare}, common term scored {common}");
        Assert.True(common > InitialScoreValue * 10, $"common term collapsed to {common}");
    }
}
