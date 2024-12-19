using System;
using System.IO;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Vector;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
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
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", VectorEmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), minimumSimilarity: 0.65f, numberOfCandidates: 12).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, 12)", q1);

                var q2 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q2);
                
                var q3 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByEmbedding([0.3f, 0.4f, 0.5f])).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q3);
                
                var q4 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithField("VectorField"), factory => factory.ByBase64("aaaa==")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(VectorField, $p0)", q4);

                var q5 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithText("TextField").TargetQuantization(VectorEmbeddingType.Int8),
                    factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text_i8(TextField), $p0)", q5);
                
                var q6 = session.Advanced.DocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", VectorEmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, null)", q6);
                
                var q7 = session.Advanced.DocumentQuery<Dto>().VectorSearch(x => x.WithText("TextField").TargetQuantization(VectorEmbeddingType.Int8),
                    factory => factory.ByText("aaaa")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text_i8(TextField), $p0)", q7);
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
                var ex1 = Assert.Throws<InvalidOperationException>(() => session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", VectorEmbeddingType.Int8).TargetQuantization(VectorEmbeddingType.Binary), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString());
                
                Assert.Contains("Cannot quantize already quantized embeddings", ex1.Message);

                var ex2 = Assert.Throws<InvalidOperationException>(() => session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", VectorEmbeddingType.Int8).TargetQuantization(VectorEmbeddingType.Single), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString());
                    
                Assert.Contains("Cannot quantize already quantized embeddings", ex2.Message);
                
                var q1 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField", VectorEmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.65, null)", q1);
                
                var q2 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(VectorEmbeddingType.Int8), factory => factory.ByEmbedding([2.5f, 3.3f]), 0.65f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.65, null)", q2);
                
                var q3 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(VectorEmbeddingType.Int8),
                        factory => factory.ByBase64("abcd=="), 0.75f).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.f32_i8(EmbeddingField), $p0, 0.75, null)", q3);

                var q4 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithText("TextField"), factory => factory.ByText("abc")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.text(TextField), $p0)", q4);

                var q5 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithBase64("Base64Field", VectorEmbeddingType.Binary), factory => factory.ByBase64("ddddd=="), 0.85f).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i1(Base64Field), $p0, 0.85, null)", q5);
                
                var q6 = session.Advanced.AsyncDocumentQuery<Dto>()
                    .VectorSearch(x => x.WithBase64("Base64Field", VectorEmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, 0.3f])).ToString();

                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(Base64Field), $p0)", q6);

                var q7 = session.Advanced.AsyncDocumentQuery<Dto>().VectorSearch(x => x.WithBase64(dto => dto.EmbeddingBase64), factory => factory.ByBase64("abcd==")).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(EmbeddingBase64, $p0)", q7);
                
                var q8 = session.Advanced.AsyncDocumentQuery<Dto>().VectorSearch(x => x.WithBase64(dto => dto.EmbeddingBase64), factory => factory.ByBase64("abcd=="), numberOfCandidates: 25, isExact: true).ToString();
                
                Assert.Equal("from 'Dtos' where exact(vector.search(EmbeddingBase64, $p0, null, 25))", q8);
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

                var q2 = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingField", VectorEmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, -0.3f]), 0.75f).ToString();
                
                Assert.Equal("from 'Dtos' where vector.search(embedding.i8(EmbeddingField), $p0, 0.75, null)", q2);

                var q3 = session.Query<Dto>().VectorSearch(x => x.WithEmbedding("EmbeddingField").TargetQuantization(VectorEmbeddingType.Int8), factory => factory.ByEmbedding([0.2f, -0.3f])).ToString();
                
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

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestQuantizer(Options options)
    {
        float[] rawEmbedding = [0.2f, 0.3f, -2.0f, 1.0f, 0.5f, -1.0f, -1.75f, 0.0f, 0.2f, 0.3f, -2.0f, 1.0f, 0.5f, -1.0f, -1.75f, 0.0f, 1.2f];
        
        var int8Embedding = VectorQuantizer.ToInt8(rawEmbedding);
        
        Assert.Equal([13, 19, -127, 64, 32, -64, -111, 0, 
                      13, 19, -127, 64, 32, -64, -111, 0, 76, /* magnitude part*/ 0, 0, 0, 64], int8Embedding);

        var magn = MathF.Abs(TensorPrimitives.MaxMagnitude(rawEmbedding));
        var magnitudeInEmbedding = MemoryMarshal.Read<float>(MemoryMarshal.Cast<sbyte, byte>(int8Embedding.AsSpan(rawEmbedding.Length)));
        Assert.Equal(magn, magnitudeInEmbedding);
        var int1Embedding = VectorQuantizer.ToInt1(rawEmbedding);
        
        Assert.Equal([217, 217, 128], int1Embedding);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestEmbeddingDimensionsCheck(Options options)
    {
        options.RunInMemory = false;
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { EmbeddingSingles = [0.5f, -1.0f] };
                var dto2 = new Dto() { EmbeddingSingles = [0.2f, 0.3f] };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();

                var index = new DummyIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var databaseDisableResult = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
                
                Assert.True(databaseDisableResult.Success);
                Assert.True(databaseDisableResult.Disabled);
                Assert.Equal(store.Database, databaseDisableResult.Name);
                
                var databaseEnableResult = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
                
                Assert.True(databaseEnableResult.Success);
                Assert.False(databaseEnableResult.Disabled);
                Assert.Equal(store.Database, databaseEnableResult.Name);
                
                var dto3 = new Dto() { EmbeddingSingles = [0.1f, 0.2f] };
                
                session.Store(dto3);
                
                session.SaveChanges();
                
                Indexes.WaitForIndexing(store);
                
                var dto4 = new Dto() { EmbeddingSingles = [0.5f, 0.7f, 0.9f] };
                
                session.Store(dto4);
                
                session.SaveChanges();

                var indexErrors = Indexes.WaitForIndexingErrors(store);
                
                Assert.Equal(1, indexErrors.Length);
                Assert.Contains("Attempted to index embedding with 3 dimensions, but field Singles already contains indexed embedding with 2 dimensions, or was explicitly configured for embeddings with 2 dimensions.", indexErrors[0].Errors[0].Error);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestInt8EmbeddingDimensionsMismatchException(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var singles1 = new float[] { 0.5f, -1.0f };
                var singles2 = new float[] { 0.5f, 0.7f };
                
                var dto1 = new Dto() { EmbeddingSBytes = VectorQuantizer.ToInt8(singles1) };
                var dto2 = new Dto() { EmbeddingSBytes = VectorQuantizer.ToInt8(singles2) };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();

                var index = new DummyIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);

                var singles3 = new float[] { 0.5f, -1.0f, 0.7f };
                var dto3 = new Dto() { EmbeddingSBytes = VectorQuantizer.ToInt8(singles3) };

                session.Store(dto3);
                
                session.SaveChanges();
                
                var indexErrors = Indexes.WaitForIndexingErrors(store);
                
                Assert.Equal(1, indexErrors.Length);
                Assert.Contains("Attempted to index embedding with 3 dimensions, but field Integers already contains indexed embedding with 2 dimensions, or was explicitly configured for embeddings with 2 dimensions.", indexErrors[0].Errors[0].Error);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestInt1EmbeddingDimensionsMismatchException(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var singles1 = new float[] { 0.5f, -1.0f };
                var singles2 = new float[] { 0.5f, -1.0f, 0.7f };
                
                var dto1 = new Dto() { EmbeddingBinary = VectorQuantizer.ToInt1(singles1) };
                var dto2 = new Dto() { EmbeddingBinary = VectorQuantizer.ToInt1(singles2) };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();

                var index = new DummyIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var singles3 = new float[] { 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f };
                var dto3 = new Dto() { EmbeddingBinary = VectorQuantizer.ToInt1(singles3) };
                
                session.Store(dto3);
                
                session.SaveChanges();
                
                var indexErrors = Indexes.WaitForIndexingErrors(store);

                Assert.Equal(1, indexErrors.Length);
                Assert.Contains("Field Binary contains embeddings with different number of dimensions.", indexErrors[0].Errors[0].Error);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestEmbeddingDimensionsMismatchExceptionWithExplicitlySetDimensions(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var singles = new float[] { 0.1f, 0.2f, 0.3f };
                var dto = new Dto() { EmbeddingSingles = singles };
                
                session.Store(dto);
                
                session.SaveChanges();

                var index = new IndexWithSetDimensions();
                
                index.Execute(store);
                
                var indexErrors = Indexes.WaitForIndexingErrors(store);
                
                Assert.Equal(1, indexErrors.Length);
                Assert.Contains("Attempted to index embedding with 3 dimensions, but field Singles already contains indexed embedding with 256 dimensions, or was explicitly configured for embeddings with 256 dimensions.", indexErrors[0].Errors[0].Error);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestEmbeddingDimensionsMismatchExceptionWithExplicitlySetDimensionsForInt8(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var singles = new float[] { 0.1f, 0.2f, 0.3f, 0.7f };
                var dto = new Dto() { EmbeddingSingles = singles };
                
                session.Store(dto);
                
                session.SaveChanges();

                var index = new IndexWithSetDimensionsInt8();
                
                index.Execute(store);
                
                var indexErrors = Indexes.WaitForIndexingErrors(store);
                
                Assert.Equal(1, indexErrors.Length);
                Assert.Contains("Attempted to index embedding with 4 dimensions, but field Sbytes already contains indexed embedding with 22 dimensions, or was explicitly configured for embeddings with 22 dimensions.", indexErrors[0].Errors[0].Error);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestAutoIndexCreationWithExactSearch(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto(){ EmbeddingSingles = new [] { 0.2f, 0.3f } };
                var queriedEmbedding = new [] { 0.2f, 0.3f };
                
                session.Store(dto);
                
                session.SaveChanges();

                _ = session.Query<Dto>().VectorSearch(x => x.WithEmbedding(d => d.EmbeddingSingles), factory => factory.ByEmbedding(queriedEmbedding), isExact: true).ToList();

                var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                
                Assert.Single(indexDefinitions);
                Assert.Equal("Auto/Dtos/ByVector.search(EmbeddingSingles)", indexDefinitions.First().Name);
            }
        }
    }

    [RavenFact(RavenTestCategory.None)]
    public void TestDefaultVectorEmbeddingType()
    {
        Assert.Equal(VectorEmbeddingType.Single, default(VectorEmbeddingType));
    }
    
    private class Dto
    {
        public string EmbeddingBase64 { get; set; }
        public float[] EmbeddingSingles { get; set; }
        public sbyte[] EmbeddingSBytes { get; set; }
        public byte[] EmbeddingBinary { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new { Singles = CreateVector(dto.EmbeddingSingles), Integers = CreateVector(dto.EmbeddingSBytes), Binary = CreateVector(dto.EmbeddingBinary) };
            
            Vector("Integers", factory => factory.SourceEmbedding(VectorEmbeddingType.Int8));
            Vector("Binary", factory => factory.SourceEmbedding(VectorEmbeddingType.Binary));
        }
    }
    
    private class IndexWithSetDimensions : AbstractIndexCreationTask<Dto>
    {
        public IndexWithSetDimensions()
        {
            Map = dtos => from dto in dtos
                select new { Singles = CreateVector(dto.EmbeddingSingles) };
            
            Vector("Singles", factory => factory.Dimensions(256));
        }
    }
    
    private class IndexWithSetDimensionsInt8 : AbstractIndexCreationTask<Dto>
    {
        public IndexWithSetDimensionsInt8()
        {
            Map = dtos => from dto in dtos
                select new { Sbytes = CreateVector(dto.EmbeddingSingles) };
            
            Vector("Sbytes", factory => factory.Dimensions(22).DestinationEmbedding(VectorEmbeddingType.Int8));
        }
    }
}
