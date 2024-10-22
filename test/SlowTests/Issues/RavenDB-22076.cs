using System;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22076 : RavenTestBase
{
    public RavenDB_22076(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestRqlGeneration(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var q1 = session.Advanced.DocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65)", q1);

                var q2 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q2);
                
                var q3 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByEmbedding([0.3f, 0.4f, 0.5f])).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q3);
                
                var q4 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByBase64("aaaa==")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q4);

                var q5 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithText("TextField").TargetQuantization(EmbeddingType.Int8),
                    factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text_i8(TextField), $p0)", q5);
                
                var q6 = session.Advanced.DocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8, VectorIndexingStrategy.HNSW), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(HNSW(embedding.i8(EmbeddingField)), $p0, 0.65)", q6);
                
                var q7 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithText("TextField", VectorIndexingStrategy.HNSW).TargetQuantization(EmbeddingType.Int8),
                    factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(HNSW(embedding.text_i8(TextField)), $p0)", q7);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestRqlGenerationAsync(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenAsyncSession())
            {
                var ex1 = Assert.Throws<Exception>(() => session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8).TargetQuantization(EmbeddingType.Binary), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString());
                
                Assert.Contains("Cannot quantize already quantized embeddings", ex1.Message);

                var ex2 = Assert.Throws<Exception>(() => session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8).TargetQuantization(EmbeddingType.Float32), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString());
                    
                Assert.Contains("Cannot quantize vector with Int8 quantization into Float32", ex2.Message);
                
                var q1 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65)", q1);
                
                var q2 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(EmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.65)", q2);
                
                var q3 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(EmbeddingType.Int8),
                        factory => factory.ByBase64("abcd=="), 0.75f).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.75)", q3);

                var q4 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithText("TextField"), factory => factory.ByText("abc")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text(TextField), $p0)", q4);

                var q5 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithBase64("Base64Field", EmbeddingType.Binary), factory => factory.ByBase64("ddddd=="), 0.85f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i1(Base64Field), $p0, 0.85)", q5);
                
                var q6 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithBase64("Base64Field", EmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, 0.3f])).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(Base64Field), $p0)", q6);

                var q7 = session.Advanced.AsyncDocumentQuery<Dto>().VectorSearch(x => x.WithBase64(dto => dto.EmbeddingBase64), factory => factory.ByBase64("abcd==")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(EmbeddingBase64, $p0)", q7);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestLinqExtensions(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var q1 = session.Query<Dto>().VectorSearch(x => x.WithText("TextField"), factory => factory.ByText("SomeText")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text(TextField), $p0)", q1);

                var q2 = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, -0.3f]), 0.75f).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.75)", q2);

                var q3 = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(EmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, -0.3f])).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.f32_i8(EmbeddingField), $p0)", q3);
                
                var q4 = session.Query<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q4);
                
                var q5 = session.Query<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByEmbedding([0.3f, 0.4f, 0.5f])).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q5);
                
                var q6 = session.Query<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByBase64("aaaa==")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q6);
            }
        }
    }

<<<<<<< HEAD
=======
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestQuantizers(Options options)
    {
        float[] rawEmbedding = [0.2f, 0.3f, -2.0f, 1.0f, 0.5f, -1.0f, -1.75f, 0.0f, 0.2f, 0.3f, -2.0f, 1.0f, 0.5f, -1.0f, -1.75f, 0.0f, 1.2f];
        
        var int8Embedding = VectorQuantizer.ToInt8(rawEmbedding);
        
        Assert.Equal([13, 19, -127, 64, 32, -64, -111, 0, 
                      13, 19, -127, 64, 32, -64, -111, 0, 76,
                      114, 34, -113, 67], int8Embedding);

        var int1Embedding = VectorQuantizer.ToInt1(rawEmbedding);
        
        Assert.Equal([217, 217, 128], int1Embedding);
    }

>>>>>>> 843c0ea2e4a (RavenDB-22076 Adjusted exceptions, changed ExactVectorSearchMatch.SimilarityI8)
    private class Dto
    {
        public string Name { get; set; }
        
        public string EmbeddingBase64 { get; set; }
    }
}
