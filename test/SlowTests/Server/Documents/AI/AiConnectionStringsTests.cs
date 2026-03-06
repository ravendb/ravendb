using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Tests.Infrastructure;
using Xunit;
#pragma warning disable SKEXP0001

namespace SlowTests.Server.Documents.AI;

public class AiConnectionStringsTests : RavenTestBase
{
    public AiConnectionStringsTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.All)]
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
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OpenAiSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OpenAiSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OpenAiSettings.Endpoint, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.AzureOpenAi:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);
                    
                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.AzureOpenAiSettings.Endpoint, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.Ollama:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OllamaSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.OllamaSettings.Uri, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings.Uri);
                    break;

                case AiConnectorType.Embedded:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    break;

                case AiConnectorType.Google:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.GoogleSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.GoogleSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings.Model);
                    break;

                case AiConnectorType.HuggingFace:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);
                    
                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.HuggingFaceSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.HuggingFaceSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings.Model);
                    break;

                case AiConnectorType.MistralAi:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.MistralAiSettings.ApiKey, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings.ApiKey);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.MistralAiSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.MistralAiSettings.Endpoint, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings.Endpoint);
                    break;
                
                case AiConnectorType.Vertex:
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].EmbeddedSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].MistralAiSettings);
                    
                    Assert.NotNull(aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.VertexSettings.GoogleCredentialsJson, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings.GoogleCredentialsJson);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.VertexSettings.Model, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings.Model);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.VertexSettings.AiVersion, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings.AiVersion);
                    Assert.Equal(embeddingsGenerationConfiguration.Connection.VertexSettings.Location, aiConnectionStringsDictionary[embeddingsGenerationConfiguration.Connection.Name].VertexSettings.Location);
                    break;
                
                default:
                    throw new ArgumentOutOfRangeException(nameof(embeddingsGenerationConfiguration.AiConnectorType), embeddingsGenerationConfiguration.AiConnectorType, null);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.NonInternal)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.Onnx, Skip = "Onnx does not require any mandatory fields.")]
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
            case AiConnectorType.Vertex:
                embeddingsGenerationConfiguration.Connection.VertexSettings.Model = string.Empty;
                break;
        }

        using (var store = GetDocumentStore())
        {
            var exception = Assert.Throws<BadRequestException>(() => store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(embeddingsGenerationConfiguration.Connection)));
            Assert.Contains($"Value of `{nameof(OpenAiSettings.Model)}` field cannot be empty.", exception.Message);
        }
    }

    private readonly List<string> _testValuesList = ["First test value", "Second test value", "Third test value"];

    [RavenMultiplatformTheory(RavenTestCategory.Etl | RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.All)]
    public void SemanticKernel_WithValidConfiguration_ShouldWork(Options options, EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration)
    {
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
        {
            (IEmbeddingGenerator<string, Embedding<float>> service, _) = AiHelper.CreateEmbeddingServicesForTest(embeddingsGenerationConfiguration);
            var embeddings = service.GenerateAsync(_testValuesList, cancellationToken: cts.Token).GetAwaiter().GetResult();

            Assert.Equal(_testValuesList.Count, embeddings.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.AzureOpenAI | RavenAiIntegration.Google)]
    [RavenAiEmbeddingsData(IntegrationType = RavenAiIntegration.Ollama | RavenAiIntegration.vLLM | RavenAiIntegration.HuggingFace | RavenAiIntegration.MistralAi | RavenAiIntegration.Onnx | RavenAiIntegration.Vertex, Skip = "This provider does not support dimensionality yet.")]
    public async Task SemanticKernel_ShouldRespect_Dimensionality(Options options, EmbeddingsGenerationConfiguration embeddingsGenerationConfiguration)
    {
        const int dimensions = 5;

        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
        {
            (IEmbeddingGenerator<string, Embedding<float>> service, _) = AiHelper.CreateEmbeddingServicesForTest(embeddingsGenerationConfiguration);
            var embeddings = await service.GenerateAsync(_testValuesList, cancellationToken: cts.Token);

            for (var i = 0; i < _testValuesList.Count; i++)
                Assert.False(embeddings[i].Vector.Length == dimensions,
                    $"{_testValuesList[i]}: Dimensionality hasn't been configured yet, but embeddings were generated with '{embeddings[i].Vector.Length}' dimensions, which should be different from {dimensions} to test it when it is configured.");

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

            (service, _) = AiHelper.CreateEmbeddingServicesForTest(embeddingsGenerationConfiguration);
            embeddings = await service.GenerateAsync(_testValuesList, cancellationToken: cts.Token);

            for (var i = 0; i < _testValuesList.Count; i++)
                Assert.True(embeddings[i].Vector.Length == dimensions, $"{_testValuesList[i]}: Dimensionality was configured to {dimensions}, but embeddings were generated with {embeddings[i].Vector.Length} dimensions.");
        }
    }
}
