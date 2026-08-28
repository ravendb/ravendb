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
    // Scoring is skipped entirely once a term's posting list grows past the small representation, so every dataset
    // here keeps one term above that size and one below it. The score buffer starts at Bm25Relevance.InitialScoreValue
    // (1/1_000_000f), so a score equal to it means BM25 contributed nothing at all.
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

    private static double[] Scores(IDocumentStore store, string where)
    {
        using var session = store.OpenSession();
        return session.Advanced
            .RawQuery<ScoreOnly>($@"from index 'Movies/Showcase' as m
                                    where {where}
                                    order by score()
                                    select {{ Score: getMetadata(m)[""@index-score""] }}
                                    limit 5")
            .ToList()
            .Select(x => x.Score)
            .ToArray();
    }

    private void AssertScored(IDocumentStore store, string where, string what)
    {
        var scores = Scores(store, where);

        Assert.NotEmpty(scores);
        output.WriteLine($"{what,-46} -> {string.Join(", ", scores.Select(s => s.ToString("G6")))}");

        foreach (var score in scores)
            Assert.False(Math.Abs(score - InitialScoreValue) < 1e-12,
                $"{what}: score is the untouched InitialScoreValue ({score}) - BM25 contributed nothing");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [InlineData(100_000, 100)]    // df = 1_000  - small posting list
    [InlineData(1_000_000, 100)]  // df = 10_000 - large posting list, sparse over 1M ids
    [InlineData(1_000_000, 10)]   // df = 100_000 - large and dense
    public void ScoreIsComputedRegardlessOfPostingListSize(int totalDocuments, int rareEvery)
    {
        using var store = SetupStore(RavenSearchEngineMode.Corax, totalDocuments, rareEvery);

        // Every clause shape below reaches a different bitmap-pipeline primitive, and each one had its own
        // posting-list shortcut that bypassed BM25: OR (single leaf), AND, AND NOT.
        AssertScored(store, "m.Genre = 'Drama'", "equals (OR fold)");
        AssertScored(store, "search(m.Title, 'matrix')", "search on analyzed field");
        AssertScored(store, "m.Genre = 'Drama' and m.Status = 'Released'", "AND of two terms");
        AssertScored(store, "m.Genre = 'Drama' and m.Status != 'Planned'", "AND NOT");
        AssertScored(store, "m.Genre = 'Drama' or m.Status = 'Released'", "OR of two terms");
        AssertScored(store, "boost(m.Genre = 'Drama', 100)", "explicit boost");
        AssertScored(store, "m.Genre in ('Drama', 'Western')", "IN");
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void CoraxScoresMatchLuceneOrderOfMagnitude()
    {
        const int documents = 1_000_000;
        const int rareEvery = 100;

        using var corax = SetupStore(RavenSearchEngineMode.Corax, documents, rareEvery);
        using var lucene = SetupStore(RavenSearchEngineMode.Lucene, documents, rareEvery);

        foreach (var where in new[] { "m.Genre = 'Drama'", "search(m.Title, 'matrix')", "m.Genre = 'Drama' and m.Status = 'Released'" })
        {
            var coraxScore = Scores(corax, where).First();
            var luceneScore = Scores(lucene, where).First();

            output.WriteLine($"{where,-50} corax={coraxScore:G6} lucene={luceneScore:G6}");

            // The formulas differ, so the scores are not equal - but they must live in the same range instead of
            // Corax collapsing to 1e-6 while Lucene reports a healthy score.
            Assert.True(coraxScore > luceneScore / 100 && coraxScore < luceneScore * 100,
                $"{where}: corax={coraxScore} vs lucene={luceneScore} - orders of magnitude apart");
        }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void ScoresStayOrderedByRelevanceAcrossPostingListSizes()
    {
        using var store = SetupStore(RavenSearchEngineMode.Corax, 1_000_000, 100);

        // Rarer term must score higher than the common one - the property that collapsed to a flat 1e-6 for every
        // term whose posting list was large.
        var rare = Scores(store, "m.Genre = 'Drama'").First();      // df = 10_000
        var common = Scores(store, "m.Status = 'Released'").First(); // df = 333_334

        output.WriteLine($"rare(df=10k)={rare:G6} common(df=333k)={common:G6}");

        Assert.True(rare > common, $"rare term scored {rare}, common term scored {common}");
        Assert.True(common > InitialScoreValue * 10, $"common term collapsed to {common}");
    }
}
