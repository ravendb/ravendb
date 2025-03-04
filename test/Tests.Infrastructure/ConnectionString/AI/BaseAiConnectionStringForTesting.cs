using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;

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
        try
        {
            var services = AiHelper.CreateServicesForTest(_embeddingsGenerationConfiguration.Value, out string serviceId);
            var embeddings = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId)
                .GenerateEmbeddingsAsync(EmbeddingsHelper.TestValuesList).Result;

            return embeddings.Count == EmbeddingsHelper.TestValuesList.Count;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
