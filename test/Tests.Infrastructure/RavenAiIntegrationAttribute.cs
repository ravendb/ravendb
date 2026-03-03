using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Util;
using Tests.Infrastructure.ConnectionString.AI;

namespace Tests.Infrastructure;

[Flags]
public enum RavenAiIntegration
{
    None = 0,
    OpenAi = 1 << 1,
    AzureOpenAI = 1 << 2,
    Ollama = 1 << 3, // we keep ollama here only for connectivity check
    Onnx = 1 << 4,
    Google = 1 << 5,
    HuggingFace = 1 << 6,
    MistralAi = 1 << 7,
    Vertex = 1 << 8,
    vLLM = 1 << 9,

    All = OpenAi | AzureOpenAI | vLLM | Onnx | Google | HuggingFace | MistralAi | Vertex,
    NonInternal = OpenAi | AzureOpenAI | vLLM | Google | HuggingFace | MistralAi | Vertex
}

public abstract class AbstractRavenAiIntegrationDataAttribute<TConfig> : RavenDataAttributeBase
    where TConfig : EtlConfiguration<AiConnectionString>
{
    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.All;
    public RavenAiIntegration IntegrationType { get; set; } = RavenAiIntegration.All;
    public object[] Data { get; set; } = null;

    protected AbstractRavenAiIntegrationDataAttribute()
    {
    }

    protected AbstractRavenAiIntegrationDataAttribute(params object[] data) : this()
    {
        Data = data;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
        {
            foreach (var aiConnectionStringForTesting in GetAiConnectionStringsSingleton(IntegrationType))
            {
                using (ResetSkipReason(Skip))
                {
                    if (string.IsNullOrEmpty(Skip))
                    {
                        SetSkipValueIfShardedDbOnX86(databaseMode);
                        SetSkipValueIfNoRequiredEnvVariablesDefined(aiConnectionStringForTesting);
                        SetSkipValueIfUnableConnectToAi(aiConnectionStringForTesting);
                    }
                    
                    var aiIntegrationConfiguration = aiConnectionStringForTesting.GetAiConfiguration();

                    if (Data == null || Data.Length == 0)
                    {
                        yield return [options, aiIntegrationConfiguration];
                        continue;
                    }

                    yield return new object[] { options, aiIntegrationConfiguration }.Concat(Data).ToArray();
                }
            }
        }
    }

    private DisposableAction ResetSkipReason(string skip) => new(() => Skip = skip);

    private void SetSkipValueIfShardedDbOnX86(RavenDatabaseMode databaseMode)
    {
        if (Is32Bit == false)
            return;

        if (databaseMode.HasFlag(RavenDatabaseMode.Sharded) == false)
            return;

        Skip = ShardingSkipMessage;
    }
    
    private void SetSkipValueIfNoRequiredEnvVariablesDefined(IAiConnectorForTesting<TConfig> aiConnectorForTesting)
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (aiConnectorForTesting.MissingRequiredEnvVariables(out var envVar) is false)
            return;
        
        Skip = $"The environment variable {envVar} is required for {aiConnectorForTesting.AiConnectorType}, but was not set.";
    }

    private void SetSkipValueIfUnableConnectToAi(IAiConnectorForTesting<TConfig> aiConnectorForTesting)
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        // we want to skip only if we cannot connect
        if (CanConnectToAi(aiConnectorForTesting, out string unableToConnectMessage))
            return;

        Skip = unableToConnectMessage;
    }

    private bool CanConnectToAi(IAiConnectorForTesting<TConfig> aiConnectorForTesting, out string skipMessage)
    {
        if (aiConnectorForTesting.CanConnect.Value)
        {
            skipMessage = Skip;
            return true;
        }

        skipMessage = $"Test requires connection to {aiConnectorForTesting.AiConnectorType.Value}.";
        return false;
    }

    public abstract IEnumerable<IAiConnectorForTesting<TConfig>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration);
}

public class RavenGenAiDataAttribute : AbstractRavenAiIntegrationDataAttribute<GenAiConfiguration>
{
    public static IEnumerable<IAiConnectorForTesting<GenAiConfiguration>> GetAiConnectionStrings(RavenAiIntegration aiIntegration)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return GenAiOpenAiConnectorForTesting.Instance;

        /*if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return GenAiOllamaConnectorForTesting.Instance;*/

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return GenAiAzureOpenAiConnectorForTesting.Instance;
    }

    public override IEnumerable<IAiConnectorForTesting<GenAiConfiguration>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration) => GetAiConnectionStrings(aiIntegration);
}

public class RavenAiEmbeddingsDataAttribute : AbstractRavenAiIntegrationDataAttribute<EmbeddingsGenerationConfiguration>
{
    public static IEnumerable<IAiConnectorForTesting<EmbeddingsGenerationConfiguration>> GetAiConnectionStrings(RavenAiIntegration aiIntegration)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return EmbeddingsOpenAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return EmbeddingsAzureOpenAiConnectorForTesting.Instance;

        /*if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return EmbeddingsOllamaConnectorForTesting.Instance;*/

        if (aiIntegration.HasFlag(RavenAiIntegration.Onnx))
            yield return EmbeddedConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Google))
            yield return EmbeddingsGoogleConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.HuggingFace))
            yield return EmbeddingsHuggingFaceConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.MistralAi))
            yield return EmbeddingsMistralAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Vertex))
            yield return EmbeddingsVertexConnectorForTesting.Instance;
    }

    public override IEnumerable<IAiConnectorForTesting<EmbeddingsGenerationConfiguration>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration) => GetAiConnectionStrings(aiIntegration);
}

