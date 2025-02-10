using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI;
#pragma warning disable SKEXP0001

namespace Tests.Infrastructure.ConnectionString.AI;

public interface IAiConnectorForTesting
{
    AiEtlConfiguration GetEtlConfiguration();
    Lazy<bool> CanConnect { get; }
    Lazy<AiConnectorType> AiConnectorType { get; }
}

public abstract class BaseAiConnectorForTesting<T> : IAiConnectorForTesting
    where T : BaseAiConnectorForTesting<T>, new()
{
    public static T Instance => field ??= new T();

    internal static T CreateNewInstance(string prefixName) => new() { NamePrefix = new Lazy<string>(prefixName) };

    private readonly Lazy<AiEtlConfiguration> _aiEtlConfiguration;

    public Lazy<bool> CanConnect { get; }

    public abstract Lazy<AiConnectorType> AiConnectorType { get; init; }

    private Lazy<string> NamePrefix { get; init; }

    protected BaseAiConnectorForTesting()
    {
        _aiEtlConfiguration = new Lazy<AiEtlConfiguration>(GetEtlConfiguration);
        CanConnect = new Lazy<bool>(CanConnectInternal);
    }

    private Lazy<string> EtlTaskName => new(() =>
    {
        var prefix = string.Empty;

        if (string.IsNullOrWhiteSpace(NamePrefix?.Value) == false)
            prefix = $"{NamePrefix.Value}_";

        return $"{prefix}{AiConnectorType.Value.ToString()}_EtlTask";
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

    public AiEtlConfiguration GetEtlConfiguration()
    {
        var connectionString = GetAiConnectionString();

        return new AiEtlConfiguration
        {
            AiConnectorType = AiConnectorType.Value,
            Name = EtlTaskName.Value,
            ConnectionStringName = ConnectionStringName.Value,
            Connection = connectionString
        };
    }

    private bool CanConnectInternal()
    {
        try
        {
            var services = AiHelper.CreateServicesForTest(_aiEtlConfiguration.Value, out string serviceId);
            var embeddings = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId)
                .GenerateEmbeddingsAsync(AiHelper.TestValuesList).Result;

            return embeddings.Count == AiHelper.TestValuesList.Count;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
