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
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f], EmbeddingType.Binary), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(i8(EmbeddingField), i1($p0), 0.65)", q1);

                var q2 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0, 0.8)", q2);
                
                var q3 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByEmbedding([0.3f, 0.4f, 0.5f], EmbeddingType.Int8)).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, i8($p0), 0.8)", q3);
                
                var q4 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByBase64("aaaa==", EmbeddingType.Binary)).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, base64(i1($p0)), 0.8)", q4);
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
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8).TargetQuantization(EmbeddingType.Binary), factory => factory.ByEmbedding([2.5f, 3.3f], EmbeddingType.Binary), 0.65f).ToString());
                
                Assert.Contains("Cannot quantize already quantized embeddings", ex1.Message);

                var ex2 = Assert.Throws<Exception>(() => session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8).TargetQuantization(EmbeddingType.Float32), factory => factory.ByEmbedding([2.5f, 3.3f], EmbeddingType.Binary), 0.65f).ToString());
                    
                Assert.Contains("Cannot quantize vector with I8 quantization into None", ex2.Message);
                
                var q1 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f], EmbeddingType.Binary), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(i8(EmbeddingField), i1($p0), 0.65)", q1);
                
                var q2 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(EmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f], EmbeddingType.Binary), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(f32_i8(EmbeddingField), i1($p0), 0.65)", q2);
                
                var q3 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(EmbeddingType.Int8),
                        factory => factory.ByBase64("abcd=="), 0.75f).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(f32_i8(EmbeddingField), base64($p0), 0.75)", q3);

                var q4 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithText("TextField"), factory => factory.ByText("abc")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(TextField, $p0, 0.8)", q4);
                
                var q5 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithBase64("Base64Field", EmbeddingType.Binary), factory => factory.ByBase64("ddddd==", EmbeddingType.Int8), 0.85f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(base64(i1(Base64Field)), base64(i8($p0)), 0.85)", q5);
                
                var q6 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithBase64("Base64Field", EmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, 0.3f])).ToString();

                Assert.Equal("from 'Dtos' where vector.search(base64(i8(Base64Field)), $p0, 0.8)", q6);
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
                
                Assert.Equal("from 'Dtos' where vector.search(TextField, $p0, 0.8)", q1);
                
                var q2 = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingField", EmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, -0.3f]), 0.75f).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(i8(EmbeddingField), $p0, 0.75)", q2);
                
                var q3 = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(EmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, -0.3f], EmbeddingType.Int8)).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(f32_i8(EmbeddingField), i8($p0), 0.8)", q3);
                
                var q4 = session.Query<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0, 0.8)", q4);
                
                var q5 = session.Query<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByEmbedding([0.3f, 0.4f, 0.5f], EmbeddingType.Int8)).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, i8($p0), 0.8)", q5);
                
                var q6 = session.Query<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByBase64("aaaa==", EmbeddingType.Binary)).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, base64(i1($p0)), 0.8)", q6);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        
        public string EmbeddingBase64 { get; set; }
    }
}
