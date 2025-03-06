using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable SKEXP0001

namespace SlowTests.Server.Documents.AI;

public class AiConnectionStringsTests : RavenTestBase
{
    private static readonly ChunkingOptions DefaultChunkingOptions = new ChunkingOptions() { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 2048 };

    public AiConnectionStringsTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    public async Task CanTestAiIntegrationScript()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var order = new Order
                {
                    Lines =
                    [
                        new OrderLine { ProductName = "Carbon replacement feather for raven wing", Quantity = 450 },
                        new OrderLine { ProductName = "Plasma gun mount for raven's foot (left-side)", Quantity = 1 }
                    ]
                };

                await session.StoreAsync(order);
                await session.SaveChangesAsync();
            }

            var connectionString = new AiConnectionString { Name = "ConnectionStringForTestingPurposes", OnnxSettings = new OnnxSettings() };
            var operation = new PutConnectionStringOperation<AiConnectionString>(connectionString);
            var putConnectionStringResult = store.Maintenance.Send(operation);
            Assert.NotNull(putConnectionStringResult.RaftCommandIndex);
            
            var configuration = new EmbeddingsGenerationConfiguration
            {
                Name = "AiIntegrationTaskForTestingPurposes",
                ConnectionStringName = "ConnectionStringForTestingPurposes",
                EmbeddingsPathConfigurations = [new EmbeddingPathConfiguration() { Path = "Lines", ChunkingOptions = DefaultChunkingOptions }],
                Collection = "Orders",
                ChunkingOptionsForQuerying = DefaultChunkingOptions
            };

            var database = await GetDatabase(store.Database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testScript = new TestEmbeddingsGenerationScript
                {
                    DocumentId = "orders/1-A",
                    Configuration = configuration
                };

                var testResult = EmbeddingsGenerationTask.TestScript(testScript, database, database.ServerStore, context);
                Assert.NotNull(testResult);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.All, CheckCanConnect = false, NightlyBuildRequired = false)]
    public void CanPutAiConnectionString_WithValidConfiguration_ShouldWork(Options options, EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration)
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(embeddingsGenerationConfiguration.Connection));

            var aiConnectionStringsDictionary = store.Maintenance.Send(new GetConnectionStringsOperation(embeddingsGenerationConfiguration.Connection.Name, ConnectionStringType.Ai)).AiConnectionStrings;

            Assert.NotNull(aiConnectionStringsDictionary);
            Assert.Equal(1, aiConnectionStringsDictionary.Count);
            Assert.True(aiConnectionStringsDictionary.ContainsKey(embeddingsGenerationConfiguration.Connection.Name));

            switch (embeddingsGenerationConfiguration.AiConnectorType)
            {
                case AiConnectorType.OpenAi:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OpenAiSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OpenAiSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OpenAiSettings.Endpoint, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.AzureOpenAi:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.Endpoint, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.Ollama:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OllamaSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OllamaSettings.Uri, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings.Uri);
                    break;

                case AiConnectorType.Onnx:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    break;

                case AiConnectorType.Google:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.GoogleSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.GoogleSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings.Model);
                    break;

                case AiConnectorType.HuggingFace:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.HuggingFaceSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.HuggingFaceSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings.Model);
                    break;

                case AiConnectorType.MistralAi:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.MistralAiSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.MistralAiSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.MistralAiSettings.Endpoint, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings.Endpoint);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(embeddingsGenerationConfiguration.AiConnectorType), embeddingsGenerationConfiguration.AiConnectorType, null);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.NonInternal, CheckCanConnect = false, NightlyBuildRequired = false)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.Onnx, Skip = "Onnx does not require any mandatory fields.")]
    public void PutAiConnectionString_WithInvalidConfiguration_ShouldThrow(Options options, EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration)
    {
        switch (embeddingsGenerationConfiguration.AiConnectorType)
        {
            case AiConnectorType.OpenAi:
                embeddingsGenerationConfiguration.Connection.OpenAiSettings.Model = string.Empty;
                break;
            case AiConnectorType.AzureOpenAi:
                embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.Model = string.Empty;
                break;
            case AiConnectorType.Ollama:
                embeddingsGenerationConfiguration.Connection.OllamaSettings.Model = string.Empty;
                break;
            case AiConnectorType.Google:
                embeddingsGenerationConfiguration.Connection.GoogleSettings.Model = string.Empty;
                break;
            case AiConnectorType.HuggingFace:
                embeddingsGenerationConfiguration.Connection.HuggingFaceSettings.Model = string.Empty;
                break;
            case AiConnectorType.MistralAi:
                embeddingsGenerationConfiguration.Connection.MistralAiSettings.Model = string.Empty;
                break;
        }
        using (var store = GetDocumentStore())
        {
            var exception = Assert.Throws<BadRequestException>(() => store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(embeddingsGenerationConfiguration.Connection)));
            Assert.Contains($"Value of `{nameof(OpenAiSettings.Model)}` field cannot be empty.", exception.Message);
        }
    }

    private readonly List<string> _testValuesList = ["First test value", "Second test value", "Third test value"];

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.All)]
    public void SemanticKernel_WithValidConfiguration_ShouldWork(Options options, EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration)
    {
        (ITextEmbeddingGenerationService service, _) = AiHelper.CreateServicesForTest(embeddingsGenerationConfiguration);
        var embeddings = AiHelper.GenerateEmbeddingsAsync(service ,_testValuesList).GetAwaiter().GetResult();

        Assert.Equal(_testValuesList.Count, embeddings.Count);
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.AzureOpenAI | RavenAiIntegration.Google)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.Ollama | RavenAiIntegration.HuggingFace | RavenAiIntegration.MistralAi | RavenAiIntegration.Onnx, Skip = "This provider does not support dimensionality yet.")]
    public void SemanticKernel_ShouldRespect_Dimensionality(Options options, EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration)
    {
        const int dimensions = 5;

        (ITextEmbeddingGenerationService service, _) = AiHelper.CreateServicesForTest(embeddingsGenerationConfiguration);
        var embeddings = AiHelper.GenerateEmbeddingsAsync(service, _testValuesList).GetAwaiter().GetResult();

        for (var i = 0; i < _testValuesList.Count; i++)
            Assert.False(embeddings[i].Length == dimensions, $"{_testValuesList[i]}: Dimensionality hasn't been configured yet, but embeddings were generated with '{embeddings[i].Length}' dimensions, which should be different from {dimensions} to test it when it is configured.");

        embeddings = null;
        Assert.Null(embeddings);

        switch (embeddingsGenerationConfiguration.AiConnectorType)
        {
            case AiConnectorType.OpenAi:
                embeddingsGenerationConfiguration.Connection.OpenAiSettings.Dimensions = dimensions;
                break;
            case AiConnectorType.AzureOpenAi:
                embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.Dimensions = dimensions;
                break;
            case AiConnectorType.Google:
                embeddingsGenerationConfiguration.Connection.GoogleSettings.Dimensions = dimensions;
                break;
        }

        (service, _) = AiHelper.CreateServicesForTest(embeddingsGenerationConfiguration);
        embeddings = AiHelper.GenerateEmbeddingsAsync(service, _testValuesList).GetAwaiter().GetResult();

        for (var i = 0; i < _testValuesList.Count; i++)
            Assert.True(embeddings[i].Length == dimensions, $"{_testValuesList[i]}: Dimensionality was configured to {dimensions}, but embeddings were generated with {embeddings[i].Length} dimensions.");
    }
}
