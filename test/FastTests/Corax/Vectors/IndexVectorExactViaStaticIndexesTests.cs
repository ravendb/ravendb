using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using SmartComponents.LocalEmbeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class IndexVectorExactViaStaticIndexesTests : RavenTestBase
{
    public IndexVectorExactViaStaticIndexesTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineData(EmbeddingType.Binary, 0.7f)]
    [InlineData(EmbeddingType.Int8, 0.55f)]
    [InlineData(EmbeddingType.Float32, 0.6f)]
    public async Task CanCreateVectorIndexFromCSharp(EmbeddingType embeddingType, float similarity)
    {
        using var store = CreateDocumentStore();
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new Document() { Text = "Cat has brown eyes." });
            await session.StoreAsync(new Document() { Text = "Apple usually is red." });
            await session.SaveChangesAsync();
        }

        await new TextVectorIndex(embeddingType).ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        {
            using var session = store.OpenAsyncSession();
            //todo: replace with linq/documentsession when available
            var results = await session
                .Advanced
                .AsyncRawQuery<Document>($"from index '{new TextVectorIndex(embeddingType).IndexName}' where vector.search(Vector, 'animal', {similarity})")
                .ToListAsync();
            Assert.Equal(1, results.Count);
            Assert.Contains("Cat", results[0].Text);
        }
    }


    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public async Task CreateVectorIndexFromFloatEmbeddings()
    {
        using var store = CreateDocumentStore();
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new Document() { Embeddings = [0.5f, 0.4f] });
            await session.StoreAsync(new Document() { Embeddings = [0.1f, 0.1f] });
            await session.StoreAsync(new Document() { Embeddings = [-0.1f, -0.1f] });
            await session.SaveChangesAsync();
            await new NumericalVectorIndex(EmbeddingType.Float32).ExecuteAsync(store);
        }

        await Indexes.WaitForIndexingAsync(store);
        WaitForUserToContinueTheTest(store);

    }

    private IDocumentStore CreateDocumentStore() => GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

    private class TextVectorIndex : AbstractIndexCreationTask<Document>
    {
        public TextVectorIndex(EmbeddingType embeddingType)
        {
            Map = docs => from doc in docs
                select new { Id = doc.Id, Vector = CreateVectorSearch(doc.Text) };


            VectorIndexes.Add(x => x.Vector, new VectorOptions() { IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Text, DestinationEmbeddingType = embeddingType});
        }
    }

    private class NumericalVectorIndex : AbstractIndexCreationTask<Document>
    {
        public NumericalVectorIndex(EmbeddingType embeddingType)
        {
            Map = docs => from doc in docs
                select new { Id = doc.Id, Vector = CreateVector(doc.Embeddings) };


            VectorIndexes.Add(x => x.Vector, new VectorOptions() { IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Float32 });
        }
    }
    
    private class Document
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public float[] Embeddings { get; set; }
        public object Vector { get; set; }
    }
}
