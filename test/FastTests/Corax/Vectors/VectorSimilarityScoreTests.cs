using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using FastTests.Voron.FixedSize;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Vector;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;
using VectorEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType;

namespace FastTests.Corax.Vectors;

public class VectorSimilarityScoreTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void TestSinglesSimilarity(int seed)
    {
        var random = new Random(seed);
        var dimensions = random.Next(7, 1600);
        using var store = GetDocumentStoreWithDocument(random, dimensions);

        using var session = store.OpenSession();
        var doc = session.Advanced.DocumentQuery<Dto>().NoTracking().NoCaching().First();

        var queryVector = Enumerable.Range(0, dimensions).Select(_ => random.NextSingle()).ToArray();

        var query = session.Query<Dto, Index>()
            .VectorSearch(f => f.WithField(d => d.Singles), v => v.ByEmbedding(queryVector), 0.1f)
            .OrderByScore();


        using IEnumerator<StreamResult<Dto>> streamResults = session.Advanced.Stream(query, out _);

        Assert.True(streamResults.MoveNext());
        Assert.NotNull(streamResults.Current);
        Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
        var similarity = (float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore);

        var expectedSimilarity = TensorPrimitives.CosineSimilarity(queryVector, doc.Singles);
        
        Assert.Equal(expectedSimilarity, similarity, 0.0001f);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(1253422244)]
    [InlineData(1351777189)]
    public void TestInt8Similarity(int seed)
    {
        var random = new Random(seed);
        var dimensions = random.Next(7, 1600);
        using var store = GetDocumentStoreWithDocument(random, dimensions);

        using var session = store.OpenSession();
        var doc = session.Advanced.DocumentQuery<Dto>().NoTracking().NoCaching().First();

        var queryVector = VectorQuantizer.ToInt8(Enumerable.Range(0, dimensions).Select(_ => random.NextSingle()).ToArray());

        var query = session.Query<Dto, Index>()
            .VectorSearch(f => f.WithField(d => d.Int8), v => v.ByEmbedding(queryVector), 0.1f)
            .OrderByScore();


        using IEnumerator<StreamResult<Dto>> streamResults = session.Advanced.Stream(query, out _);

        Assert.True(streamResults.MoveNext());
        Assert.NotNull(streamResults.Current);
        Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
        var similarity = (float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore);
        
        var expectedSimilarity = 1 - Hnsw.CosineSimilarityI8(MemoryMarshal.Cast<sbyte, byte>(queryVector), MemoryMarshal.Cast<sbyte, byte>(doc.Int8));
        Assert.Equal(expectedSimilarity, similarity, 0.0001f);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void TestBinarySimilarity(int seed)
    {
        var random = new Random(seed);
        var dimensions = random.Next(7, 1600);
        using var store = GetDocumentStoreWithDocument(random, dimensions);

        using var session = store.OpenSession();
        var doc = session.Advanced.DocumentQuery<Dto>().NoTracking().NoCaching().First();
        byte[] queryVector;
        var commonsBits = -1L;
        do
        {
            queryVector = VectorQuantizer.ToInt1(Enumerable.Range(0, dimensions).Select(_ => random.NextSingle()).ToArray());
            commonsBits = TensorPrimitives.HammingBitDistance<byte>(queryVector, doc.Binary);
        } while (commonsBits <= 0);

        var query = session.Query<Dto, Index>()
            .VectorSearch(f => f.WithField(d => d.Binary), v => v.ByEmbedding(queryVector), 0.01f)
            .OrderByScore();
        
        using IEnumerator<StreamResult<Dto>> streamResults = session.Advanced.Stream(query, out _);

        Assert.True(streamResults.MoveNext());
        Assert.NotNull(streamResults.Current);
        Assert.NotNull(streamResults.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
        var similarity = (float)streamResults.Current.Metadata.GetDouble(Constants.Documents.Metadata.IndexScore);
        
        var hammingBitDistance = (queryVector.Length * 8f - commonsBits) / (queryVector.Length * 8f);
        
        Assert.Equal(hammingBitDistance, similarity, 0.0001f);
    }

    private IDocumentStore GetDocumentStoreWithDocument(Random random, int dimension)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };

        var store = GetDocumentStore(options);
        using var session = store.OpenSession();

        var singles = Enumerable.Range(0, dimension).Select(x => (random.NextInt64() % 2 == 0 ? -1 : 1 ) * random.NextSingle()).ToArray();
        var int8 = VectorQuantizer.ToInt8(singles);
        var binary = VectorQuantizer.ToInt1(singles);

        session.Store(new Dto(singles, int8, binary));
        session.SaveChanges();
        
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
        
        return store;
    }

    private record Dto(float[] Singles, sbyte[] Int8, byte[] Binary);

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from doc in dtos
                select new { Singles = CreateVector(doc.Singles), Int8 = CreateVector(doc.Int8), Binary = CreateVector(doc.Binary) };

            Vector(f => f.Int8, i => i.SourceEmbedding(VectorEmbeddingType.Int8));
            Vector(f => f.Binary, i => i.SourceEmbedding(VectorEmbeddingType.Binary));
        }
    }
}
