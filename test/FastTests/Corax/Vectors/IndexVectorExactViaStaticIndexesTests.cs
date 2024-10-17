using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
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

    [RavenFact(RavenTestCategory.Corax)]
    public async Task AssertIndexDefinictionViaStaticIndexes()
    {
        using var store = CreateDocumentStore();
        var localIndexDefinition = new IndexDefinition()
        {
            Name = "Vector",
            Maps = new HashSet<string>()
            {
                @"from doc in docs.Vector
select new 
{
    vec = CreateVectorSearch(doc.Vector)
}
"
            },
            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                {
                    "Vec",
                    new IndexFieldOptions()
                    {
                        Vector = new VectorOptions()
                        {
                            DestinationEmbeddingType = EmbeddingType.Binary,
                            SourceEmbeddingType = EmbeddingType.Binary,
                            Dimensions = 1,
                            IndexingStrategy = VectorIndexingStrategy.HNSW
                        }
                    }
                }
            }
        };

        await store.Maintenance.SendAsync(new PutIndexesOperation(localIndexDefinition));

        var indexDefinitionFromServer = (await store.Maintenance.SendAsync(new GetIndexesOperation(0, 1))).First();


        var cmp = localIndexDefinition.Compare(indexDefinitionFromServer);
        Assert.Equal(IndexDefinitionCompareDifferences.None, cmp);
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
            var res = session.Query<Document, TextVectorIndex>().VectorSearch(x => x.WithField(f => f.Vector), f => f.ByText("animal"), similarity);
            var results = await res.ToListAsync();

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
        public TextVectorIndex()
        {
            //querying
        }

        public TextVectorIndex(EmbeddingType embeddingType)
        {
            Map = docs => from doc in docs
                select new { Id = doc.Id, Vector = CreateVector(doc.Text) };


            VectorIndexes.Add(x => x.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Text, DestinationEmbeddingType = embeddingType
                });
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

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingTextSourceTest() => StaticIndexApi<EmbeddingTextSource>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingTextIndexTest() => StaticIndexApi<MultiEmbeddingTextIndex>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingSingleIndexTest() => StaticIndexApi<EmbeddingSingleIndex>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingSingleIndexTest() => StaticIndexApi<MultiEmbeddingSingleIndex>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingSingleAsBase64IndexTest() => StaticIndexApi<EmbeddingSingleAsBase64Index>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingSingleAsBase64IndexTest() => StaticIndexApi<MultiEmbeddingSingleAsBase64Index>();

    private void StaticIndexApi<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = CreateDocumentStore();
        using (var session = store.OpenSession())
        {
            float[][] embeddings = [[0.1f, 0.1f], [0.2f, 0.3f]];
            var embAsByte = MemoryMarshal.Cast<float, byte>(embeddings[0]).ToArray();
            var embAsByte2 = MemoryMarshal.Cast<float, byte>(embeddings[1]).ToArray();
            session.Store(new DataSource()
            {
                Text = "cat",
                MultiText = ["cat", "dog"],
                Embeddings = embeddings[0],
                MultipleEmbeddings = embeddings,
                EmbeddingAsBase64 = Convert.ToBase64String(embAsByte),
                EmbeddingsAsBase64 = [Convert.ToBase64String(embAsByte), Convert.ToBase64String(embAsByte2)]
            });
            session.SaveChanges();
            WaitForUserToContinueTheTest(store);
        }

        new TIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        Assert.Equal(0, GetErrorCounts());

        int GetErrorCounts()
        {
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation());
            return errors.First(x => x.Name == new TIndex().IndexName).Errors.Length;
        }
    }

    private class DataSource
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public string[] MultiText { get; set; }
        public float[] Embeddings { get; set; }
        public float[][] MultipleEmbeddings { get; set; }
        public string EmbeddingAsBase64 { get; set; }
        public string[] EmbeddingsAsBase64 { get; set; }
        public object Vector { get; set; }
    }


    private class EmbeddingTextSource : AbstractIndexCreationTask<DataSource>
    {
        public EmbeddingTextSource()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.Text) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Text, DestinationEmbeddingType = EmbeddingType.Float32
                });
        }
    }

    private class MultiEmbeddingTextIndex : AbstractIndexCreationTask<DataSource>
    {
        public MultiEmbeddingTextIndex()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.MultiText) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Text, DestinationEmbeddingType = EmbeddingType.Float32
                });
        }
    }

    private class EmbeddingSingleIndex : AbstractIndexCreationTask<DataSource>
    {
        public EmbeddingSingleIndex()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.Embeddings) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Float32, DestinationEmbeddingType = EmbeddingType.Float32
                });
        }
    }

    private class MultiEmbeddingSingleIndex : AbstractIndexCreationTask<DataSource>
    {
        public MultiEmbeddingSingleIndex()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.MultipleEmbeddings) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Float32, DestinationEmbeddingType = EmbeddingType.Float32
                });
        }
    }

    private class EmbeddingSingleAsBase64Index : AbstractIndexCreationTask<DataSource>
    {
        public EmbeddingSingleAsBase64Index()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.EmbeddingAsBase64) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Float32, DestinationEmbeddingType = EmbeddingType.Float32
                });
        }
    }

    private class MultiEmbeddingSingleAsBase64Index : AbstractIndexCreationTask<DataSource>
    {
        public MultiEmbeddingSingleAsBase64Index()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.EmbeddingsAsBase64) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    IndexingStrategy = VectorIndexingStrategy.Exact, SourceEmbeddingType = EmbeddingType.Float32, DestinationEmbeddingType = EmbeddingType.Float32
                });
        }
    }
}
