using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class MultiVectorSearchClientAPI(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CanSearchByMultipleVectorsByTexts()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { Name = "pizza" });
            session.Store(new Dto() { Name = "car" });
            session.Store(new Dto() { Name = "beach" });
            session.SaveChanges();

            var multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.Name), v => v.ByText(["italian food", "vehicle"])).ToList();
            Assert.Equal(2, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.Name), v => v.ByText(["italian food", "dog"])).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithText(s => s.Name), v => v.ByText(["cat", "dog"])).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }

        new VectorStaticIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.Name), v => v.ByText(["italian food", "vehicle"])).ToList();
            Assert.Equal(2, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.Name), v => v.ByText(["italian food", "dog"])).ToList();
            Assert.Equal(1, multiVectorTextualQuery.Count);

            multiVectorTextualQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithField(s => s.Name), v => v.ByText(["cat", "dog"])).ToList();
            Assert.Equal(0, multiVectorTextualQuery.Count);
        }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CanSearchByMultipleVectorsByEmbeddings()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { Vector = GetEmbedding("pizza") });
            session.Store(new Dto() { Vector = GetEmbedding("car") });
            session.Store(new Dto() { Vector = GetEmbedding("beach") });
            session.SaveChanges();

            var multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("vehicle")])).ToList();
            Assert.Equal(2, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("dog")])).ToList();
            Assert.Equal(1, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("cat"), GetEmbedding("dog")])).ToList();
            Assert.Equal(0, multiVectorEmbeddingQuery.Count);
        }

        new VectorStaticIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("vehicle")])).ToList();
            Assert.Equal(2, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("dog")])).ToList();
            Assert.Equal(1, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("cat"), GetEmbedding("dog")])).ToList();
            Assert.Equal(0, multiVectorEmbeddingQuery.Count);
        }

        float[] GetEmbedding(string input)
        {
            using var embedding = GenerateEmbeddings.FromText(bsc, VectorOptions.DefaultText, input);

            return MemoryMarshal.Cast<byte, float>(embedding.GetEmbedding()).ToArray();
        }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CanSearchByMultipleVectorsByRavenVector()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { Vector = GetEmbedding("pizza").Embedding });
            session.Store(new Dto() { Vector = GetEmbedding("car").Embedding });
            session.Store(new Dto() { Vector = GetEmbedding("beach").Embedding });
            session.SaveChanges();

            var multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("vehicle")])).ToList();
            Assert.Equal(2, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("dog")])).ToList();
            Assert.Equal(1, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("cat"), GetEmbedding("dog")])).ToList();
            Assert.Equal(0, multiVectorEmbeddingQuery.Count);
        }

        new VectorStaticIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("vehicle")])).ToList();
            Assert.Equal(2, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("italian food"), GetEmbedding("dog")])).ToList();
            Assert.Equal(1, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByEmbedding([GetEmbedding("cat"), GetEmbedding("dog")])).ToList();
            Assert.Equal(0, multiVectorEmbeddingQuery.Count);
        }

        RavenVector<float> GetEmbedding(string input)
        {
            using var embedding = GenerateEmbeddings.FromText(bsc, VectorOptions.DefaultText, input);

            return MemoryMarshal.Cast<byte, float>(embedding.GetEmbedding()).ToArray();
        }
    }
    
     [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CanSearchByMultipleVectorsByBase64()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { Vector = GetEmbeddingForStore("pizza").Embedding });
            session.Store(new Dto() { Vector = GetEmbeddingForStore("car").Embedding });
            session.Store(new Dto() { Vector = GetEmbeddingForStore("beach").Embedding });
            session.SaveChanges();

            var multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByBase64([GetEmbedding("italian food"), GetEmbedding("vehicle")])).ToList();
            Assert.Equal(2, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByBase64([GetEmbedding("italian food"), GetEmbedding("dog")])).ToList();
            Assert.Equal(1, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto>().Customize(p => p.WaitForNonStaleResults())
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByBase64([GetEmbedding("cat"), GetEmbedding("dog")])).ToList();
            Assert.Equal(0, multiVectorEmbeddingQuery.Count);
        }

        new VectorStaticIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByBase64([GetEmbedding("italian food"), GetEmbedding("vehicle")])).ToList();
            Assert.Equal(2, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByBase64([GetEmbedding("italian food"), GetEmbedding("dog")])).ToList();
            Assert.Equal(1, multiVectorEmbeddingQuery.Count);

            multiVectorEmbeddingQuery = session.Query<Dto, VectorStaticIndex>()
                .VectorSearch(f => f.WithEmbedding(s => s.Vector), v => v.ByBase64([GetEmbedding("cat"), GetEmbedding("dog")])).ToList();
            Assert.Equal(0, multiVectorEmbeddingQuery.Count);
        }

        string GetEmbedding(string input)
        {
            using var embedding = GenerateEmbeddings.FromText(bsc, VectorOptions.DefaultText, input);

            return Convert.ToBase64String(embedding.GetEmbedding());
        }
        
        RavenVector<float> GetEmbeddingForStore(string input)
        {
            using var embedding = GenerateEmbeddings.FromText(bsc, VectorOptions.DefaultText, input);

            return MemoryMarshal.Cast<byte, float>(embedding.GetEmbedding()).ToArray();
        }
    }

    private class VectorStaticIndex : AbstractIndexCreationTask<Dto>
    {
        public VectorStaticIndex()
        {
            Map = dtos => dtos.Select(x => new { Name = CreateVector(x.Name), Vector = CreateVector(x.Vector) });
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        public float[] Vector { get; set; }
    }
}
