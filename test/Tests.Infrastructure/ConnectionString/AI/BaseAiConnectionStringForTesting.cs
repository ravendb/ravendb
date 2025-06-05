using System;
using System.Threading;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Conventions;
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
    Lazy<AiConnectorType> AiConnectorType { get; }
    bool MissingRequiredApiKey(out string environmentVariableName);
}

public abstract class BaseAiConnectorForTesting<T, TConfig> : IAiConnectorForTesting<TConfig>
    where T : BaseAiConnectorForTesting<T, TConfig>, new()
    where TConfig : AbstractAiIntegrationConfiguration, new()
{
    private static T _instance;

    public static T Instance => _instance ??= new T();

    internal static T CreateNewInstance(string prefixName) => new() { NamePrefix = new Lazy<string>(prefixName) };

    protected readonly Lazy<TConfig> _aiIntegrationConfiguration;

    public Lazy<bool> CanConnect { get; }

    public abstract Lazy<AiConnectorType> AiConnectorType { get; init; }

    protected string[] RequiredEnvironmentVariables = [];

    public virtual bool MissingRequiredApiKey(out string environmentVariableName)
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

    private Lazy<string> NamePrefix { get; init; }

    protected BaseAiConnectorForTesting()
    {
        _aiIntegrationConfiguration = new Lazy<TConfig>(GetAiConfiguration);
        CanConnect = new Lazy<bool>(IsConnectionAllowed);
    }

    protected Lazy<string> AiIntegrationTaskName => new(() =>
    {
        var prefix = string.Empty;

        if (string.IsNullOrWhiteSpace(NamePrefix?.Value) == false)
            prefix = $"{NamePrefix.Value}_";

        return $"{prefix}{AiConnectorType.Value.ToString()}_AiIntegrationTask";
    });

    protected Lazy<string> ConnectionStringName => new(() =>
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
        connectionString.Identifier = Guid.NewGuid().ToString();

        return connectionString;
    }

    public TConfig GetAiConfiguration()
    {
        var connectionString = GetAiConnectionString();

        return new TConfig
        {
            Name = AiIntegrationTaskName.Value,
            ConnectionStringName = ConnectionStringName.Value,
            Connection = connectionString
        };
    }

    private bool IsConnectionAllowed()
    {
        InMemoryLoggerProvider logger = null;

        try
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
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
                Console.WriteLine($"ERROR: Unable to connect to {AiConnectorType.Value} due to the following error:{Environment.NewLine}{errorDetails}");
            }

            return false;
        }
        finally
        {
            logger?.Dispose();
        }
    }
    protected abstract bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token);

}

public abstract class AbstractEmbeddingsConnectorForTesting<T> : BaseAiConnectorForTesting<T, EmbeddingsGenerationConfiguration>
    where T : AbstractEmbeddingsConnectorForTesting<T>, new()
{
    protected override bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token)
    {
        logger = null;

        (ITextEmbeddingGenerationService service, logger) = AiHelper.CreateEmbeddingServicesForTest(_aiIntegrationConfiguration.Value);
        var embeddings = service.GenerateEmbeddingsAsync(EmbeddingsHelper.ValuesListToVerifyConnection, cancellationToken: token).GetAwaiter().GetResult();

        var isExpectedResponse = embeddings.Count == EmbeddingsHelper.ValuesListToVerifyConnection.Count;
        if (isExpectedResponse == false)
            Console.WriteLine(
                $"ERROR: Unexpected response from {AiConnectorType.Value}: '{embeddings.Count}' embeddings were generated for '{EmbeddingsHelper.ValuesListToVerifyConnection.Count}' input values.");

        return isExpectedResponse;
    }
}

public abstract class AbstractGenAiConnectorForTesting<T> : BaseAiConnectorForTesting<T, GenAiConfiguration>
    where T : AbstractGenAiConnectorForTesting<T>, new()
{
    protected override bool TryConnect(out InMemoryLoggerProvider logger, CancellationToken token)
    {
        var configuration = _aiIntegrationConfiguration.Value;
        configuration.JsonSchema = ChatCompletionClient.GetSchemaFor("{ \"Answer\" : \"answer here\" }");
        using (var contextPool = new JsonContextPool())
        using (var client = ChatCompletionClient.CreateChatCompletionClient(contextPool, configuration))
        {
            logger = null;
            var result = client.CompleteAsync(prompt: "Reply with exact word only: raven", "", token).GetAwaiter().GetResult();

            return true;

        }
    }
}


