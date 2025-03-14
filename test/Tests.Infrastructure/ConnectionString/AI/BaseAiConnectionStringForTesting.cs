using System;
using System.Threading;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

#pragma warning disable SKEXP0001

namespace Tests.Infrastructure.ConnectionString.AI;

public interface IAiConnectorForTesting
{
    EmbeddingsGenerationConfiguration GetEtlConfiguration();
    Lazy<bool> CanConnect { get; }
    Lazy<AiConnectorType> AiConnectorType { get; }
}

public abstract class BaseAiConnectorForTesting<T> : IAiConnectorForTesting
    where T : BaseAiConnectorForTesting<T>, new()
{
    private static T _instance;

    public static T Instance => _instance ??= new T();

    internal static T CreateNewInstance(string prefixName) => new() { NamePrefix = new Lazy<string>(prefixName) };

    private readonly Lazy<EmbeddingsGenerationConfiguration> _embeddingsGenerationConfiguration;

    public Lazy<bool> CanConnect { get; }

    public abstract Lazy<AiConnectorType> AiConnectorType { get; init; }

    private Lazy<string> NamePrefix { get; init; }

    protected BaseAiConnectorForTesting()
    {
        _embeddingsGenerationConfiguration = new Lazy<EmbeddingsGenerationConfiguration>(GetEtlConfiguration);
        CanConnect = new Lazy<bool>(CanConnectInternal);
    }

    private Lazy<string> AiIntegrationTaskName => new(() =>
    {
        var prefix = string.Empty;

        if (string.IsNullOrWhiteSpace(NamePrefix?.Value) == false)
            prefix = $"{NamePrefix.Value}_";

        return $"{prefix}{AiConnectorType.Value.ToString()}_AiIntegrationTask";
    });

    private Lazy<string> ConnectionStringName => new(() =>
    {
        var prefix = string.Empty;

        if (string.IsNullOrWhiteSpace(NamePrefix?.Value) == false)
            prefix = $"{NamePrefix.Value}_";

        return $"{prefix}{AiConnectorType.Value.ToString()}_ConnectionString";
    });

    protected abstract AiConnectionString CreateAiConnectionStringImpl();

    public AiConnectionString GetAiConnectionString()
    {
        var connectionString = CreateAiConnectionStringImpl();
        connectionString.Name = ConnectionStringName.Value;

        return connectionString;
    }

    public EmbeddingsGenerationConfiguration GetEtlConfiguration()
    {
        var connectionString = GetAiConnectionString();

        return new EmbeddingsGenerationConfiguration
        {
            Name = AiIntegrationTaskName.Value,
            ConnectionStringName = ConnectionStringName.Value,
            Connection = connectionString
        };
    }

    private bool CanConnectInternal()
    {
        InMemoryLoggerProvider logger = null;

        try
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                (ITextEmbeddingGenerationService service, logger) = AiHelper.CreateServicesForTest(_embeddingsGenerationConfiguration.Value);
                var embeddings = AiHelper.GenerateEmbeddingsAsync(service, EmbeddingsHelper.ValuesListToVerifyConnection, cts.Token).GetAwaiter().GetResult();

                var isExpectedResponse = embeddings.Count == EmbeddingsHelper.ValuesListToVerifyConnection.Count;
                if (isExpectedResponse == false)
                    Console.WriteLine(
                        $"ERROR: Unexpected response from {AiConnectorType.Value}: '{embeddings.Count}' embeddings were generated for '{EmbeddingsHelper.ValuesListToVerifyConnection.Count}' input values.");

                return isExpectedResponse;
            }
        }
        catch (Exception e)
        {
            var errorDetailsJson = new DynamicJsonValue
            {
                [nameof(NodeConnectionTestResult.Error)] = e.Message,
                [nameof(e.StackTrace)] = e.StackTrace
            };

            if (logger != null)
            {
                var logsArray = new DynamicJsonArray(collection: logger.GetLogs());
                errorDetailsJson[nameof(NodeConnectionTestResult.Log)] = logsArray;
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var errorDetails = context.ReadObject(errorDetailsJson, "error").ToString();
                Console.WriteLine($"ERROR: Unable to connect to {AiConnectorType.Value} due to the following error:{Environment.NewLine}{errorDetails}");
            }

            return false;

        }
        finally
        {
            logger?.Dispose();
        }
    }
}
