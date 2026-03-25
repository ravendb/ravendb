using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25698(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    public async Task EmbeddingGenerationCreatesValueInEmbeddingCacheDocument()
    {

        using var store = GetDocumentStore();
        
        var aiConnectionString = new AiConnectionString { Name = "aiconnection", EmbeddedSettings = new EmbeddedSettings() };
        aiConnectionString.Identifier = aiConnectionString.GenerateIdentifier();
        var embeddingsGenerationConfiguration = new EmbeddingsGenerationConfiguration
        {
            Name = "task",
            ConnectionStringName = aiConnectionString.Identifier,
            EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = new ChunkingOptions(){ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048} }],
            Collection = "Products",
            EmbeddingsTransformation = null,
            Quantization = VectorEmbeddingType.Single,
            ChunkingOptionsForQuerying = new(){ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048},
        };
        embeddingsGenerationConfiguration.Identifier = embeddingsGenerationConfiguration.GenerateIdentifier();
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(aiConnectionString));
        var taskCreated = await store.Maintenance.SendAsync(new AddEtlOperation<AiConnectionString>(embeddingsGenerationConfiguration));

        var complete = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Product("Milk"));
            await session.SaveChangesAsync();
        }
        
        Assert.True(await complete.WaitAsync(TimeSpan.FromMinutes(1)));
        
        WaitForUserToContinueTheTest(store);
        using (var session = store.OpenAsyncSession())
        {
            var cache = await session.LoadAsync<JObject>("embeddings-cache/aiconnection/g7Nyh12ZRGaWFoDWXdQyKFM+rgcMJhFP4UIUVs9RYnI=");
            Assert.Equal("Milk", cache.Value<string>("Value"));
        }
    }

    private record Product(string Name);
}

