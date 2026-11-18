using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26975(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class Article
    {
        public string Body { get; set; }
    }

    private sealed class Articles_ByBody : AbstractIndexCreationTask<Article>
    {
        public Articles_ByBody()
        {
            Map = articles => from a in articles
                              select new { a.Body };

            Index(x => x.Body, FieldIndexing.Search);

            Configuration = new IndexConfiguration
            {
                ["Indexing.Corax.IncludeDocumentScore"] = "true"
            };
        }
    }

    private void Seed(Options options, out Raven.Client.Documents.IDocumentStore store)
    {
        store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            // Different relevance to "raven": high term frequency in a short body, a single hit in a short
            // body, and a single hit buried in a long body. Only the long body also contains "once".
            session.Store(new Article { Body = "raven raven raven raven raven" });
            session.Store(new Article { Body = "raven database" });
            session.Store(new Article { Body = "a long article that mentions raven exactly once among many other unrelated words spread across a much longer body of text" });
            session.SaveChanges();
        }

        new Articles_ByBody().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private static double[] ScoresFor(IDocumentSession session, string searchTerms)
    {
        var hits = session.Query<Article, Articles_ByBody>()
            .Search(x => x.Body, searchTerms)
            .OrderByScore()
            .ToList();

        return hits
            .Select(a => Convert.ToDouble(session.Advanced.GetMetadataFor(a)["@index-score"]))
            .ToArray();
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void FullTextSearch_OrderByScore_ScoreMustVaryByRelevance(Options options)
    {
        Seed(options, out var store);
        using (store)
        using (var session = store.OpenSession())
        {
            var scores = ScoresFor(session, "raven");

            Assert.Equal(3, scores.Length);

            // The bug (RavenDB-26975): Corax 2.0 collapsed the search() into a flat-scored bitmap, so every
            // document came back with an identical ~1.0 score and relevance ordering was lost.
            Assert.NotEqual(1, scores.Distinct().Count());

            // order by score() (default) is highest-relevance-first, so scores must be non-increasing,
            // and the 5x-"raven" document must rank first.
            for (var i = 1; i < scores.Length; i++)
                Assert.True(scores[i - 1] >= scores[i], $"scores not descending: [{string.Join(", ", scores)}]");
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MultiTermSearch_OrderByScore_ScoreMustVaryByRelevance(Options options)
    {
        Seed(options, out var store);
        using (store)
        using (var session = store.OpenSession())
        {
            // search(Body, 'raven once') is an OR over two terms. BM25 sums per-term relevance, so the score
            // must reflect both the term frequency of "raven" and the (rarer, higher-idf) hit on "once".
            var scores = ScoresFor(session, "raven once");

            Assert.Equal(3, scores.Length);

            // Multi-term search must still produce varying, non-flat scores (not the collapsed bitmap score).
            Assert.NotEqual(1, scores.Distinct().Count());

            for (var i = 1; i < scores.Length; i++)
                Assert.True(scores[i - 1] >= scores[i], $"scores not descending: [{string.Join(", ", scores)}]");
        }
    }
}
