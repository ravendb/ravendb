using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class VectorSearchTokenAliasTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class VecDoc
    {
        public string Id { get; set; }
        public float[] Vector { get; set; }
        public string Name { get; set; }
    }

    private class VecResult
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public double Score { get; set; }
    }

    private class VecIndex : AbstractIndexCreationTask<VecDoc>
    {
        public VecIndex()
        {
            Map = docs => from doc in docs
                select new Entry
                {
                    Vector = CreateVector(doc.Vector),
                    Name = doc.Name
                };

            VectorIndexes.Add(x => ((Entry)(object)x).Vector,
                new VectorOptions { SourceEmbeddingType = VectorEmbeddingType.Single });
        }

        public class Entry
        {
            public object Vector { get; set; }
            public string Name { get; set; }
        }
    }

    private void SeedDocs(IDocumentStore store)
    {
        using var session = store.OpenSession();
        session.Store(new VecDoc { Vector = new float[] { 0.1f, 0.2f }, Name = "match" });
        session.Store(new VecDoc { Vector = new float[] { -0.9f, -0.9f }, Name = "nomatch" });
        session.SaveChanges();

        new VecIndex().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void VectorSearchTokenSurvivesFromAliasWhenProjectionIsJsObject()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        SeedDocs(store);

        using var session = store.OpenSession();

        var query = session.Query<VecIndex.Entry, VecIndex>()
            .VectorSearch(f => f.WithField(e => e.Vector), v => v.ByEmbedding(new float[] { 0.1f, 0.2f }), minimumSimilarity: 0.5f)
            .OrderByScore()
            .OfType<VecDoc>()
            .Select(d => new VecResult
            {
                Id = d.Id,
                Name = d.Name,
                Score = (double)RavenQuery.Metadata(d)[Constants.Documents.Metadata.IndexScore]
            });

        string rql = query.ToString();

        Assert.Contains("vector.search(d.Vector,", rql);
        Assert.DoesNotContain("d.Vector = ", rql);

        var results = query.ToList();

        Assert.Single(results);
        Assert.Equal("match", results[0].Name);
        Assert.True(results[0].Score > 0);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void ExactVectorSearchTokenSurvivesFromAliasWhenProjectionIsJsObject()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        SeedDocs(store);

        using var session = store.OpenSession();

        var query = session.Query<VecIndex.Entry, VecIndex>()
            .VectorSearch(f => f.WithField(e => e.Vector), v => v.ByEmbedding(new float[] { 0.1f, 0.2f }), minimumSimilarity: 0.5f, isExact: true)
            .OrderByScore()
            .OfType<VecDoc>()
            .Select(d => new VecResult
            {
                Id = d.Id,
                Name = d.Name,
                Score = (double)RavenQuery.Metadata(d)[Constants.Documents.Metadata.IndexScore]
            });

        string rql = query.ToString();

        Assert.Contains("exact(vector.search(d.Vector,", rql);
        Assert.DoesNotContain("d.Vector = ", rql);

        var results = query.ToList();

        Assert.Single(results);
        Assert.Equal("match", results[0].Name);
        Assert.True(results[0].Score > 0);
    }
}
