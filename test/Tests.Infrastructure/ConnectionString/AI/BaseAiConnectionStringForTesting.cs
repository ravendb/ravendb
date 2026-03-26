using System;
using System.Threading;
using Microsoft.Extensions.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

#pragma warning disable SKEXP0001

namespace Tests.Infrastructure.ConnectionString.AI;

public interface IAiConnectorForTesting<TConfig>
where TConfig : EtlConfiguration<AiConnectionString>
{
    TConfig GetAiConfiguration();
    Lazy<bool> CanConnect { get; }
    bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token);
    AiConnectorType AiConnectorType { get; }
    bool MissingRequiredEnvVariables(out string environmentVariableName);
}

public abstract class BaseAiConnectorForTesting<T, TConfig> : IAiConnectorForTesting<TConfig>
    where T : BaseAiConnectorForTesting<T, TConfig>, new()
    where TConfig : AbstractAiIntegrationConfiguration, new()
{
    private static T _instance;

    public static T Instance => _instance ??= new T();

    protected readonly TConfig _aiIntegrationConfiguration;

    public Lazy<bool> CanConnect { get; }

    public abstract AiConnectorType AiConnectorType { get; init; }

    protected string[] RequiredEnvironmentVariables = [];

    public virtual bool MissingRequiredEnvVariables(out string environmentVariableName)
    {
        foreach (var envVar in RequiredEnvironmentVariables)
        {
            if (Environment.GetEnvironmentVariable(envVar) == null)
            {
                environmentVariableName = envVar;
                return true;
            }
        }

        environmentVariableName = null;
        return false;
    }

    protected string NamePrefix { get; init; }

    protected BaseAiConnectorForTesting()
    {
        _aiIntegrationConfiguration = GetAiConfiguration();
        CanConnect = new Lazy<bool>(IsConnectionAllowed);
    }

    protected string AiIntegrationTaskName
    {
        get
        {
            var prefix = string.IsNullOrWhiteSpace(NamePrefix) ? string.Empty : $"{NamePrefix}_";
            return $"{prefix}{AiConnectorType}_AiIntegrationTask";
        }
    }

    protected string ConnectionStringName
    {
        get
        {
            var prefix = string.IsNullOrWhiteSpace(NamePrefix) ? string.Empty : $"{NamePrefix}_";
            return $"{prefix}{AiConnectorType}_ConnectionString";
        }
    }

    protected abstract AiConnectionString CreateAiConnectionStringImpl();

    public AiConnectionString GetAiConnectionString()
    {
        var connectionString = CreateAiConnectionStringImpl();
        connectionString.Name = ConnectionStringName;
        connectionString.Identifier = Guid.NewGuid().ToString();

        return connectionString;
    }

    public TConfig GetAiConfiguration()
    {
        var connectionString = GetAiConnectionString();

        return new TConfig
        {
            Name = AiIntegrationTaskName,
            ConnectionStringName = ConnectionStringName,
            Connection = connectionString
        };
    }

    private bool IsConnectionAllowed()
    {
        InMemoryLoggerProvider logger = null;

        try
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
            {
                return TryConnect(out logger, cts.Token);
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
                Console.WriteLine($"ERROR: Unable to connect to {AiConnectorType} due to the following error:{Environment.NewLine}{errorDetails}");
            }

            return false;
        }
        finally
        {
            logger?.Dispose();
        }
    }
    public abstract bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token);

}

public abstract class AbstractEmbeddingsConnectorForTesting<T> : BaseAiConnectorForTesting<T, EmbeddingsGenerationConfiguration>
    where T : AbstractEmbeddingsConnectorForTesting<T>, new()
{
    public override bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token)
    {
        logger = null;

        (IEmbeddingGenerator<string, Embedding<float>> service, logger) = AiHelper.CreateEmbeddingServicesForTest(_aiIntegrationConfiguration);
        var embeddings = service.GenerateAsync(EmbeddingsHelper.ValuesListToVerifyConnection, cancellationToken: token).GetAwaiter().GetResult();

        var isExpectedResponse = embeddings.Count == EmbeddingsHelper.ValuesListToVerifyConnection.Count;
        if (isExpectedResponse == false)
            Console.WriteLine(
                $"ERROR: Unexpected response from {AiConnectorType}: '{embeddings.Count}' embeddings were generated for '{EmbeddingsHelper.ValuesListToVerifyConnection.Count}' input values.");

        return isExpectedResponse;
    }
}

public abstract class AbstractGenAiConnectorForTesting<T> : BaseAiConnectorForTesting<T, GenAiConfiguration>
    where T : AbstractGenAiConnectorForTesting<T>, new()
{
    public override bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token)
    {
        var configuration = _aiIntegrationConfiguration;
        var schema = ChatCompletionClient.GetSchemaFromSampleObject("{ \"Answer\" : \"answer here\" }");
        using (var contextPool = new JsonContextPool())
        using (var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, configuration.Connection))
        {
            logger = null;
            client.TestCompleteAsync(systemPrompt: "Reply with exact word only: raven", "", schema, token).GetAwaiter().GetResult();
            return true;
        }
    }
}


