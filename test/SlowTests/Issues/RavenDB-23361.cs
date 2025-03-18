using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23361 : RavenTestBase
{
    public RavenDB_23361(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void TestAutoIndexesMerging(Options options)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle)] = "0";
        };
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                _ = session.Query<Dto>().Where(x => x.EmbeddingBase64 == "abcd").ToList();
                
                Indexes.WaitForIndexing(store);
                
                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                
                Assert.Single(indexes);
                Assert.Single(indexes[0].Fields.Keys);
                Assert.Contains("EmbeddingBase64", indexes[0].Fields.Keys);

                var queriedEmbedding = new float[] { 0.5f, 0.1f };
                
                _ = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingSingles"), factory => factory.ByEmbedding(queriedEmbedding)).Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5))).ToList();
                
                Indexes.WaitForIndexing(store);
                
                indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                
                Assert.Equal(2, indexes.Length);

                var vectorSearchIndex = indexes.Single(x => x.Name == "Auto/Dtos/ByVector.search(EmbeddingSingles)");

                Assert.Single(vectorSearchIndex.Fields);
                Assert.Contains("vector.search(EmbeddingSingles)", vectorSearchIndex.Fields.Keys);

                _ = session.Query<Dto>().Where(x => x.EmbeddingBinary.Contains((byte)0)).Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5))).ToList();
                
                Indexes.WaitForIndexing(store);

                indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                
                Assert.Equal(2, indexes.Length);
                
                var nonVectorSearchIndex = indexes.Single(x => x.Name == "Auto/Dtos/ByEmbeddingBase64AndEmbeddingBinary");
                Assert.Equal(2, nonVectorSearchIndex.Fields.Count);
                Assert.Contains("EmbeddingBase64", nonVectorSearchIndex.Fields.Keys);
                Assert.Contains("EmbeddingBinary", nonVectorSearchIndex.Fields.Keys);
                
                _ = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingSingles"), factory => factory.ByEmbedding(queriedEmbedding)).Where(x => x.EmbeddingBase64 == "abcd").Customize(x => x.WaitForNonStaleResults()).ToList();
                
                Indexes.WaitForIndexing(store);
                
                indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                
                Assert.Equal(2, indexes.Length);
                
                // assert non-VectorSearch index stays the same
                nonVectorSearchIndex = indexes.Single(x => x.Name == "Auto/Dtos/ByEmbeddingBase64AndEmbeddingBinary");
                Assert.Equal(2, nonVectorSearchIndex.Fields.Count);
                Assert.Contains("EmbeddingBase64", nonVectorSearchIndex.Fields.Keys);
                Assert.Contains("EmbeddingBinary", nonVectorSearchIndex.Fields.Keys);
                
                vectorSearchIndex = indexes.Single(x => x.Name == "Auto/Dtos/ByEmbeddingBase64AndVector.search(EmbeddingSingles)");
                Assert.Equal(2, vectorSearchIndex.Fields.Count);
                Assert.Contains("vector.search(EmbeddingSingles)", vectorSearchIndex.Fields.Keys);
                Assert.Contains("EmbeddingBase64", vectorSearchIndex.Fields.Keys);
                
                _ = session.Query<Dto>().Where(x => x.EmbeddingBase64 == "abcd").Statistics(out var statistics).ToList();
                
                Assert.Equal("Auto/Dtos/ByEmbeddingBase64AndEmbeddingBinary", statistics.IndexName);
            }
        }
    }
    
    private class Dto
    {
        public string EmbeddingBase64 { get; set; }
        public float[] EmbeddingSingles { get; set; }
        public sbyte[] EmbeddingSBytes { get; set; }
        public byte[] EmbeddingBinary { get; set; }
    }
}
