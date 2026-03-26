using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Static;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RavenDB_25000(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanUseEmbeddingGenerationTaskInIndexEntriesQuery()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var etl = Etl.WaitForEtlToComplete(store);
        var cityConfig = new EmbeddingPathConfiguration()
        {
            Path = "ShipTo.City",
            ChunkingOptions = new ChunkingOptions()
            {
                ChunkingMethod = ChunkingMethod.PlainTextSplit,
                MaxTokensPerChunk = 2048
            }
        };

        var countryConfig = new EmbeddingPathConfiguration()
        {
            Path = "ShipTo.Country",
            ChunkingOptions = new ChunkingOptions()
            {
                ChunkingMethod = ChunkingMethod.PlainTextSplitLines,
                MaxTokensPerChunk = 2048
            }
        };

        using (var session = store.OpenSession())
        {
            session.Store(new Order() { ShipTo = new Address() { City = "London", Country = "UK" } });
            session.SaveChanges();
        }

        var (configuration, _) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [cityConfig, countryConfig], collectionName: "Orders");
        await etl.WaitAsync(DefaultEtlTimeout);
        var index = new Index();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);

        using (var commands = store.Commands())
        {
            var command = new QueryCommand(commands.Session, new IndexQuery
            {
                Query = $@"from index '{index.IndexName}' where vector.search(VectorCity, ""uk"", 0.82, 20)"
            });

            await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

            var results = new DynamicArray(command.Result.Results);
            Assert.Equal(1, results.Count());
        }

        using (var commands = store.Commands())
        {
            var command = new QueryCommand(commands.Session, new IndexQuery
            {
                Query = $@"from index '{index.IndexName}' where vector.search(VectorCity, ""uk"", 0.82, 20)"
            }, indexEntriesOnly: true);

            await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

            var results = new DynamicArray(command.Result.Results);
            Assert.Equal(1, results.Count());
        }
    }

    private class Index : AbstractIndexCreationTask<Order>
    {
        public Index()
        {
            Map = docs => from doc in docs
                select new
                {
                    VectorCity = LoadVector("ShipTo.City", "localaitask"),
                    VectoryCountry = LoadVector("ShipTo.Country", "localaitask")
                };
        }
    }
}
