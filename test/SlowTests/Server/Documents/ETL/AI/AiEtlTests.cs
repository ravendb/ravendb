using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable SKEXP0001

namespace SlowTests.Server.Documents.ETL.AI;

public class AiEtlTests : RavenTestBase
{
    public AiEtlTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.AiIntegration)]
    public async Task CanTestAiEtlScript()
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

            var transformation = new Transformation
            {
                Name = "TransformationForTestingPurposes",
                Script = "loadToWhatever(){}",
                Collections = ["Orders"]
            };

            var aiEtlConfiguration = new AiEtlConfiguration
            {
                Name = "EtlForTestingPurposes",
                ConnectionStringName = "ConnectionStringForTestingPurposes",
                PathsToProcess = ["Lines"],
                Transforms = [transformation],
            };

            var database = await GetDatabase(store.Database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testScript = new TestAiEtlScript
                {
                    DocumentId = "orders/1-A",
                    Configuration = aiEtlConfiguration
                };

                var testResult = AiEtl.TestScript(testScript, database, database.ServerStore, context);
                Assert.NotNull(testResult);
            }
        }
    }

    [RavenTheory(RavenTestCategory.AiIntegration)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.All, CheckCanConnect = false, NightlyBuildRequired = false)]
    public void CanPutAiConnectionString_WithValidConfiguration_ShouldWork(Options options, AiEtlConfiguration aiEtlConfiguration)
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(aiEtlConfiguration.Connection));

            var aiConnectionStringsDictionary = store.Maintenance.Send(new GetConnectionStringsOperation(aiEtlConfiguration.Connection.Name, ConnectionStringType.Ai)).AiConnectionStrings;

            Assert.NotNull(aiConnectionStringsDictionary);
            Assert.Equal(1, aiConnectionStringsDictionary.Count);
            Assert.True(aiConnectionStringsDictionary.ContainsKey(aiEtlConfiguration.Connection.Name));

            switch (aiEtlConfiguration.AiConnectorType)
            {
                case AiConnectorType.OpenAi:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Equal(aiEtlConfiguration.Connection.OpenAiSettings.ApiKey, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings.ApiKey);
                    Assert.Equal(aiEtlConfiguration.Connection.OpenAiSettings.Model, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings.Model);
                    Assert.Equal(aiEtlConfiguration.Connection.OpenAiSettings.Endpoint, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.AzureOpenAi:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Equal(aiEtlConfiguration.Connection.AzureOpenAiSettings.ApiKey, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings.ApiKey);
                    Assert.Equal(aiEtlConfiguration.Connection.AzureOpenAiSettings.Model, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings.Model);
                    Assert.Equal(aiEtlConfiguration.Connection.AzureOpenAiSettings.Endpoint, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings.Endpoint);
                    break;

                case AiConnectorType.Ollama:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Equal(aiEtlConfiguration.Connection.OllamaSettings.Model, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings.Model);
                    Assert.Equal(aiEtlConfiguration.Connection.OllamaSettings.Uri, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings.Uri);
                    break;

                case AiConnectorType.Onnx:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    break;

                case AiConnectorType.Google:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Equal(aiEtlConfiguration.Connection.GoogleSettings.ApiKey, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings.ApiKey);
                    Assert.Equal(aiEtlConfiguration.Connection.GoogleSettings.Model, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings.Model);
                    break;

                case AiConnectorType.HuggingFace:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);
                    Assert.Equal(aiEtlConfiguration.Connection.HuggingFaceSettings.ApiKey, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings.ApiKey);
                    Assert.Equal(aiEtlConfiguration.Connection.HuggingFaceSettings.Model, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings.Model);
                    break;

                case AiConnectorType.MistralAi:
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].AzureOpenAiSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OllamaSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].OnnxSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].GoogleSettings);
                    Assert.Null(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].HuggingFaceSettings);

                    Assert.NotNull(aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings);
                    Assert.Equal(aiEtlConfiguration.Connection.MistralAiSettings.ApiKey, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings.ApiKey);
                    Assert.Equal(aiEtlConfiguration.Connection.MistralAiSettings.Model, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings.Model);
                    Assert.Equal(aiEtlConfiguration.Connection.MistralAiSettings.Endpoint, aiConnectionStringsDictionary[aiEtlConfiguration.Connection.Name].MistralAiSettings.Endpoint);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(aiEtlConfiguration.AiConnectorType), aiEtlConfiguration.AiConnectorType, null);
            }
        }
    }

    [RavenTheory(RavenTestCategory.AiIntegration)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.NonInternal, CheckCanConnect = false, NightlyBuildRequired = false)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.Onnx, Skip = "Onnx does not require any mandatory fields.")]
    public void PutAiConnectionString_WithInvalidConfiguration_ShouldThrow(Options options, AiEtlConfiguration aiEtlConfiguration)
    {
        switch (aiEtlConfiguration.AiConnectorType)
        {
            case AiConnectorType.OpenAi:
                aiEtlConfiguration.Connection.OpenAiSettings.Model = string.Empty;
                break;
            case AiConnectorType.AzureOpenAi:
                aiEtlConfiguration.Connection.AzureOpenAiSettings.Model = string.Empty;
                break;
            case AiConnectorType.Ollama:
                aiEtlConfiguration.Connection.OllamaSettings.Model = string.Empty;
                break;
            case AiConnectorType.Google:
                aiEtlConfiguration.Connection.GoogleSettings.Model = string.Empty;
                break;
            case AiConnectorType.HuggingFace:
                aiEtlConfiguration.Connection.HuggingFaceSettings.Model = string.Empty;
                break;
            case AiConnectorType.MistralAi:
                aiEtlConfiguration.Connection.MistralAiSettings.Model = string.Empty;
                break;
        }
        using (var store = GetDocumentStore())
        {
            var exception = Assert.Throws<BadRequestException>(() => store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(aiEtlConfiguration.Connection)));
            Assert.Contains($"Value of `{nameof(OpenAiSettings.Model)}` field cannot be empty.", exception.Message);
        }
    }

    private readonly List<string> _testValuesList = ["First test value", "Second test value", "Third test value"];

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.AiIntegration)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.All)]
    public void SemanticKernel_WithValidConfiguration_ShouldWork(Options options, AiEtlConfiguration aiEtlConfiguration)
    {
        var services = AiHelper.CreateServicesForTest(aiEtlConfiguration, out string serviceId);
        var embeddings = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId)
            .GenerateEmbeddingsAsync(_testValuesList).Result;

        Assert.Equal(_testValuesList.Count, embeddings.Count);
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.AiIntegration)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.AzureOpenAI | RavenAiIntegration.Onnx | RavenAiIntegration.Google)]
    [RavenAiIntegrationData(IntegrationType = RavenAiIntegration.Ollama | RavenAiIntegration.HuggingFace | RavenAiIntegration.MistralAi, Skip = "This provider does not support dimensionality yet.")]
    public void SemanticKernel_ShouldRespect_Dimensionality(Options options, AiEtlConfiguration aiEtlConfiguration)
    {
        const int dimensions = 5;

        var services = AiHelper.CreateServicesForTest(aiEtlConfiguration, out string serviceId);
        var embeddings = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId)
            .GenerateEmbeddingsAsync(_testValuesList).Result;

        for (var i = 0; i < _testValuesList.Count; i++)
            Assert.False(embeddings[i].Length == dimensions, $"{_testValuesList[i]}: Dimensionality was not configured yet, but embeddings were generated with {embeddings[i].Length} dimensions.");

        embeddings = null;
        Assert.Null(embeddings);

        switch (aiEtlConfiguration.AiConnectorType)
        {
            case AiConnectorType.OpenAi:
                aiEtlConfiguration.Connection.OpenAiSettings.Dimensions = dimensions;
                break;
            case AiConnectorType.AzureOpenAi:
                aiEtlConfiguration.Connection.AzureOpenAiSettings.Dimensions = dimensions;
                break;
            case AiConnectorType.Onnx:
                aiEtlConfiguration.Connection.OnnxSettings.Dimensions = dimensions;
                break;
            case AiConnectorType.Google:
                aiEtlConfiguration.Connection.GoogleSettings.Dimensions = dimensions;
                break;
        }

        services = AiHelper.CreateServicesForTest(aiEtlConfiguration, out serviceId);
        embeddings = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId)
            .GenerateEmbeddingsAsync(_testValuesList).Result;

        for (var i = 0; i < _testValuesList.Count; i++)
            Assert.True(embeddings[i].Length == dimensions, $"{_testValuesList[i]}: Dimensionality was configured to {dimensions}, but embeddings were generated with {embeddings[i].Length} dimensions.");
    }
}
