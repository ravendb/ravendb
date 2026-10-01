using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class VectorSearchDocumentBoost(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public void SingleVectorClauseResultsAreMultipliedByDocumentBoost(bool multiVector, bool includeDocumentScore)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = includeDocumentScore.ToString();
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item([1f, 1f], Boost: 9), "items/1");
            session.Store(new Item([1f, 0f], Boost: 1), "items/2");
            session.SaveChanges();
        }

        new Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Query<Item, Index>()
                .VectorSearch(f => f.WithField(x => x.Vector), v =>
                {
                    if (multiVector)
                        v.ByEmbeddings(new[] { new[] { 1f, 0f }, new[] { 0f, 1f } });
                    else
                        v.ByEmbedding(new[] { 1f, 0f });
                })
                .ToList();

            Assert.Equal(new[] { "items/1", "items/2" }, results.Select(x => session.Advanced.GetDocumentId(x)));
            if (includeDocumentScore == false)
                return;

            var scores = results.Select(x => session.Advanced.GetMetadataFor(x).GetDouble(Constants.Documents.Metadata.IndexScore)).ToArray();
            Assert.Equal(MathF.Sqrt(0.5f) * MathF.Log(9 + 1), scores[0], 0.0001f);
            Assert.Equal(1, scores[1], 0.0001f);
        }
    }

    private record Item(float[] Vector, float Boost);

    private class Index : AbstractIndexCreationTask<Item>
    {
        public Index()
        {
            Map = items => from item in items
                select new { Vector = CreateVector(item.Vector) }.Boost(item.Boost);
        }
    }
}
