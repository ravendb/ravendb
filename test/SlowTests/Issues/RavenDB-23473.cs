using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23473(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CanIndexVectorWhenPreviousElementsAreNullWithoutExplicitVectorFieldConfiguration()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Embedding() { };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        new DtoIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var dto = session.Load<Embedding>(id);
            dto.Vector = [.1f, .2f, .3f];
            session.Store(dto);
            session.SaveChanges();
        }

        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public async Task CanUpdateNoExplicitlyConfiguredVectorFieldViaSubscriptionWithLoadDocument()
    {
        using (var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax)))
        {
            await store.ExecuteIndexAsync(new VectorIndex());

            var sub = await store.Subscriptions.CreateAsync<Question>(new SubscriptionCreationOptions<Question>());
            var worker = store.Subscriptions.GetSubscriptionWorker<Question>(sub);
            var mre = new AsyncManualResetEvent();
            List<float> vector = [];
            var t = worker.Run(async x =>
            {
                using var session = x.OpenAsyncSession();
                foreach (var item in x.Items)
                {
                    var q = item.Result;
                    var hash = Hashing.XXHash64.Calculate(Encodings.Utf8.GetBytes(q.Body));
                    var embeddingId = $"embeddings/{hash}";

                    var localEmbedding = await session.LoadAsync<Embedding>(embeddingId);
                    if (localEmbedding == null)
                    {
#pragma warning disable SKEXP0070
                        var embedding = await GenerateEmbeddings.Embedder.Value.GenerateEmbeddingsAsync(new List<string> {q.Body });
#pragma warning restore SKEXP0070
                        vector = embedding[0].ToArray().ToList();
                        localEmbedding = new Embedding { Vector = vector };
                        await session.StoreAsync(localEmbedding, embeddingId);
                    }

                    q.EmbeddingId = localEmbedding.Id;
                }

                await session.SaveChangesAsync();
                mre.Set();
            });

            for (int i = 0; i < 5; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Question { Body = "this is a test" });
                    await session.SaveChangesAsync();
                }
            }

            await mre.WaitAsync();

            await Indexes.WaitForIndexingAsync(store);
            var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
            Assert.Null(errors);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session.Query<Question, VectorIndex>()
                    .VectorSearch(
                        f => f.WithField("Vector"),
                        v => v.ByEmbedding(vector))
                    .ToListAsync();
                Assert.Equal(5, results.Count);
            }
            
            var deleteByQueryOp = await store.Operations.SendAsync(new DeleteByQueryOperation("from 'embeddings'"));
            await deleteByQueryOp.WaitForCompletionAsync();
            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Advanced.AsyncDocumentQuery<Question, VectorIndex>().WhereEquals("Vector", null).ToListAsync();
                Assert.Equal(5, result.Count);
            }            
        }
    }

    private class VectorIndex : AbstractIndexCreationTask<Question>
    {
        public VectorIndex()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;

            Map = questions =>
                from q in questions
                let embedding = LoadDocument<Embedding>(q.EmbeddingId)
                select new { Vector = CreateVector(embedding.Vector) };
        }
    }
    
    private class DtoIndex : AbstractIndexCreationTask<Embedding>
    {
        public DtoIndex()
        {
            Map = dtos => from dto in dtos select new { Vector = CreateVector(dto.Vector) };
        }
    }
    
    private class Embedding
    {
        public string Id { get; set; }
        public List<float> Vector { get; set; }
    }

    private class Question
    {
        public string Body { get; set; }
        public string EmbeddingId { get; set; }
    }
}
