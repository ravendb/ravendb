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
    Ollama = 1 << 3,
    Onnx = 1 << 4,
    Google = 1 << 5,
    HuggingFace = 1 << 6,
    MistralAi = 1 << 7,
    Vertex = 1 << 8,

    All = OpenAi | AzureOpenAI | Ollama | Onnx | Google | HuggingFace | MistralAi | Vertex,
    NonInternal = OpenAi | AzureOpenAI | Ollama | Google | HuggingFace | MistralAi | Vertex
}

public abstract class AbstractRavenAiIntegrationDataAttribute<TConfig> : RavenDataAttributeBase
    where TConfig : EtlConfiguration<AiConnectionString>
{
    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.All;
    public RavenAiIntegration IntegrationType { get; set; } = RavenAiIntegration.All;
    public object[] Data { get; set; } = null;
    public bool NightlyBuildRequired { get; set; } = true;
    public bool CheckCanConnect { get; set; } = true;
    public bool ReuseConnectionString { get; set; } = true;

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
            Func<IEnumerable<IAiConnectorForTesting<TConfig>>> aiConnectionStringForTestingGetter = ReuseConnectionString
                ? () => GetAiConnectionStringsSingleton(IntegrationType)
                : () => GetAiConnectionStringsNewInstance(IntegrationType, testMethod.Name);

            foreach (var aiConnectionStringForTesting in aiConnectionStringForTestingGetter.Invoke())
            {
                using (SetSkipValueIfNightlyBuildRequired())
                using (SetSkipValueIfShardedDbOnX86(databaseMode))
                using (SetSkipValueIfNoRequiredEnvVariablesDefined(aiConnectionStringForTesting))
                using (SetSkipValueIfUnableConnectToAi(aiConnectionStringForTesting))
                {
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

    private DisposableAction SetSkipValueIfShardedDbOnX86(RavenDatabaseMode databaseMode)
    {
        if (string.IsNullOrEmpty(Skip) == false)
            return null;

        if (Is32Bit == false)
            return null;

        if (databaseMode.HasFlag(RavenDatabaseMode.Sharded) == false)
            return null;

        Skip = ShardingSkipMessage;
        return new DisposableAction(() => Skip = null);
    }
    
    
    private DisposableAction SetSkipValueIfNoRequiredEnvVariablesDefined(IAiConnectorForTesting<TConfig> aiConnectorForTesting)
    {
        if (string.IsNullOrEmpty(Skip) == false)
            return null;

        if (aiConnectorForTesting.MissingRequiredEnvVariables(out var envVar) is false)
            return null;
        
        Skip = $"The environment variable {envVar} is required for {aiConnectorForTesting.AiConnectorType}, but was not set.";
        return new DisposableAction(() => Skip = null);
    }

    private DisposableAction SetSkipValueIfUnableConnectToAi(IAiConnectorForTesting<TConfig> aiConnectorForTesting)
    {
        if (string.IsNullOrEmpty(Skip) == false)
            return null;

        if (CheckCanConnect == false)
            return null;

        if (CanConnectToAi(aiConnectorForTesting, out string unableToConnectMessage))
            return null;

        Skip = unableToConnectMessage;
        return new DisposableAction(() => Skip = null);
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

    private DisposableAction SetSkipValueIfNightlyBuildRequired()
    {
        if (string.IsNullOrEmpty(Skip) == false)
            return null;

        if (NightlyBuildRequired == false || NightlyBuildTheoryAttribute.IsNightlyBuild)
            return null;

        Skip = NightlyBuildTheoryAttribute.SkipMessage;
        return new DisposableAction(() => Skip = null);
    }

    public abstract IEnumerable<IAiConnectorForTesting<TConfig>> GetAiConnectionStringsNewInstance(RavenAiIntegration aiIntegration, string testMethodName);
    public abstract IEnumerable<IAiConnectorForTesting<TConfig>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration);
}

public class RavenGenAiDataAttribute : AbstractRavenAiIntegrationDataAttribute<GenAiConfiguration>
{
    public override IEnumerable<IAiConnectorForTesting<GenAiConfiguration>> GetAiConnectionStringsNewInstance(RavenAiIntegration aiIntegration, string testMethodName)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return GenAiOpenAiConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return GenAiOllamaConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return GenAiAzureOpenAiConnectorForTesting.CreateNewInstance(testMethodName);
    }

    public override IEnumerable<IAiConnectorForTesting<GenAiConfiguration>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return GenAiOpenAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return GenAiOllamaConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return GenAiAzureOpenAiConnectorForTesting.Instance;
    }
}

public class RavenAiEmbeddingsDataAttribute : AbstractRavenAiIntegrationDataAttribute<EmbeddingsGenerationConfiguration>
{
    public override IEnumerable<IAiConnectorForTesting<EmbeddingsGenerationConfiguration>> GetAiConnectionStringsNewInstance(RavenAiIntegration aiIntegration, string testMethodName)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return EmbeddingsOpenAiConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return EmbeddingsAzureOpenAiConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return EmbeddingsOllamaConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.Onnx))
            yield return EmbeddedConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.Google))
            yield return EmbeddingsGoogleConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.HuggingFace))
            yield return EmbeddingsHuggingFaceConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.MistralAi))
            yield return EmbeddingsMistralAiConnectorForTesting.CreateNewInstance(testMethodName);
        
        if (aiIntegration.HasFlag(RavenAiIntegration.Vertex))
            yield return EmbeddingsVertexConnectorForTesting.CreateNewInstance(testMethodName);
    }

    public override IEnumerable<IAiConnectorForTesting<EmbeddingsGenerationConfiguration>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return EmbeddingsOpenAiConnectorForTesting.Instance;
        
        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return EmbeddingsAzureOpenAiConnectorForTesting.Instance;
        
        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return EmbeddingsOllamaConnectorForTesting.Instance;
        
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
}

