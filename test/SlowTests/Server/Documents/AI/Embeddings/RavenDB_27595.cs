using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RavenDB_27595(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    private const string Text = "raven wing feather";

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.All, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TestScript_UsesConfiguredConnectionString(Options options, EmbeddingsGenerationConfiguration configuration)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Dto { Name = Text }, "dtos/1");
            await session.SaveChangesAsync();
        }

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));

        var connectorType = configuration.AiConnectorType;
        configuration.Connection = null;
        configuration.Collection = "Dtos";
        configuration.EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration { Path = nameof(Dto.Name), ChunkingOptions = DefaultChunkingOptions }];
        configuration.ChunkingOptionsForQuerying = DefaultChunkingOptions;

        var database = await GetDatabase(store.Database);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var testScript = new TestEmbeddingsGenerationScript { DocumentId = "dtos/1", Configuration = configuration };

            var result = (EmbeddingsGenerationTestScriptResult)EmbeddingsGenerationTask.TestScript(testScript, database, database.ServerStore, context);

            var item = Assert.Single(result.Results[nameof(Dto.Name)]);
            Assert.Equal(Text, item.Value);

            var embedding = MemoryMarshal.Cast<byte, float>(item.Embeddings.Span).ToArray();
            var onnxEmbedding = GenerateEmbeddingForTextViaOnnx(Text);
            if (connectorType == AiConnectorType.Embedded)
                Assert.Equal(onnxEmbedding, embedding);
            else
                Assert.NotEqual(onnxEmbedding, embedding);
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
}
