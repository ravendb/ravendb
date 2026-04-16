using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Sharding.Queries;

public class RavenDB_26266(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly float[] QueryVector = [1f, 0f];
    private static readonly float[] HighSimilarityVector = [1.0f, 0.0f];
    private static readonly float[] MedSimilarityVector = [0.8f, 0.6f];
    private static readonly float[] LowSimilarityVector = [0.6f, 0.8f];

    private static readonly float[] MultiQueryVector1 = [1f, 0f];
    private static readonly float[] MultiQueryVector2 = [0f, 1f];
    private static readonly float[] MultiHighVector = [1.00f, 0.00f];
    private static readonly float[] MultiSecondHighVector = [0.28f, 0.96f];
    private static readonly float[] MultiMedVector = [0.80f, 0.60f];

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Sharding)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void ShardedSingleVectorSearchResultsAreSortedByScoreWithExplicitOrderByScore(Options options) =>
            RunVectorSearchTest(options, [HighSimilarityVector, MedSimilarityVector, LowSimilarityVector], session =>
                session.Advanced
                    .RawQuery<VecDoc>("from 'VecDocs' where vector.search(Vector, $q) order by score()")
                    .AddParameter("q", QueryVector)
                    .WaitForNonStaleResults()
                    .ToList());


    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Sharding)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void ShardedSingleVectorSearchResultsAreSortedByScoreWithoutOrderByScore(Options options) => RunVectorSearchTest(options, [HighSimilarityVector, MedSimilarityVector, LowSimilarityVector], session =>
            session.Advanced
                .RawQuery<VecDoc>("from 'VecDocs' where vector.search(Vector, $q)")
                .AddParameter("q", QueryVector)
                .WaitForNonStaleResults()
                .ToList());


    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Sharding)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void MultiVectorSearchResultsAreSortedByScoreWithExplicitOrderByScore(Options options) => RunVectorSearchTest(options, [MultiHighVector, MultiMedVector, MultiSecondHighVector], session =>
                session.Query<VecDoc>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .VectorSearch(f => f.WithEmbedding(s => s.Vector),
                        v => v.ByEmbeddings([MultiQueryVector1, MultiQueryVector2]))
                    .OrderByScore()
                    .ToList());


    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Sharding)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void MultiVectorSearchResultsAreSortedByScoreWithoutOrderByScore(Options options) => RunVectorSearchTest(options, [MultiHighVector, MultiMedVector, MultiSecondHighVector], session =>
                session.Query<VecDoc>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .VectorSearch(f => f.WithEmbedding(s => s.Vector),
                        v => v.ByEmbeddings([MultiQueryVector1, MultiQueryVector2]))
                    .ToList());


    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Sharding)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void ShardedVectorSearchWithOrderByScoreAndAdditionalField(Options options)
    {
        using var store = GetDocumentStore(options);
        var config = Sharding.GetShardingConfiguration(store);

        var sameSimilarityVector = new float[] { 0.8f, 0.6f };
        var docs = new (float[] Vector, string Name)[]
        {
            (HighSimilarityVector, "Charlie"),
            (sameSimilarityVector, "Bob"),
            (sameSimilarityVector, "Alice"),
        };

        using (var session = store.OpenSession())
        {
            for (var i = 0; i < docs.Length; i++)
            {
                var id = Sharding.GetRandomIdForShard(config, i);
                session.Store(new NamedVecDoc { Vector = docs[i].Vector, Name = docs[i].Name }, id);
            }

            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var results = session.Advanced
                .RawQuery<NamedVecDoc>("from 'NamedVecDocs' where vector.search(Vector, $q) order by score(), Name")
                .AddParameter("q", QueryVector)
                .WaitForNonStaleResults()
                .ToList();

            Assert.Equal(docs.Length, results.Count);
            Assert.Equal("Charlie", results[0].Name);
            Assert.Equal("Alice", results[1].Name);
            Assert.Equal("Bob", results[2].Name);
        }
    }

    private void RunVectorSearchTest(Options options, float[][] vectors, Func<IDocumentSession, List<VecDoc>> query)
    {
        using var store = GetDocumentStore(options);
        var config = Sharding.GetShardingConfiguration(store);

        using (var session = store.OpenSession())
        {
            for (var i = 0; i < vectors.Length; i++)
            {
                var id = Sharding.GetRandomIdForShard(config, i);
                session.Store(new VecDoc { Vector = vectors[i] }, id);
            }

            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var results = query(session);

            Assert.Equal(vectors.Length, results.Count);

            var scores = results
                .Select(r => Convert.ToDouble(session.Advanced.GetMetadataFor(r)["@index-score"]))
                .ToList();

            for (var i = 0; i < scores.Count - 1; i++)
            {
                Assert.True(scores[i] >= scores[i + 1]);
            }
        }
    }

    private class VecDoc
    {
        public float[] Vector { get; set; }
    }

    private class NamedVecDoc
    {
        public float[] Vector { get; set; }
        public string Name { get; set; }
    }
}
