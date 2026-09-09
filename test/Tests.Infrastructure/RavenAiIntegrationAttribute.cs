using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Util;
using Raven.Server.Config;
using Tests.Infrastructure.ConnectionString.AI;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

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
    vLLM = 1 << 9,

    All = OpenAi | AzureOpenAI | vLLM | Onnx | Google | HuggingFace | MistralAi | Vertex,
    NonInternal = OpenAi | AzureOpenAI | vLLM | Google | HuggingFace | MistralAi | Vertex
}

public abstract class AbstractRavenAiIntegrationDataAttribute<TConfig> : RavenDataAttributeBase
    where TConfig : EtlConfiguration<AiConnectionString>
{
    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.All;
    public RavenAiIntegration IntegrationType { get; set; } = RavenAiIntegration.All;
    public object[] Data { get; set; }

    protected AbstractRavenAiIntegrationDataAttribute()
    {
    }

    protected AbstractRavenAiIntegrationDataAttribute(params object[] data) : this()
    {
        Data = data;
    }

    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var result = new List<ITheoryDataRow>();
        foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
        {
            foreach (var aiConnectionStringForTesting in GetAiConnectionStringsSingleton(IntegrationType))
            {
                using (ResetSkipReason(Skip))
                {
                    if (string.IsNullOrEmpty(Skip) && aiConnectionStringForTesting.AiConnectorType == AiConnectorType.Ollama)
                    {
                        Skip = "Local Ollama integration tests are currently disabled.";
                    }
                    else if (HasSkipReason(aiConnectionStringForTesting) == false)
                    {
                        if (aiConnectionStringForTesting.CanConnect.Value == false)
                        {
                            Skip = $"Test requires connection to {aiConnectionStringForTesting.AiConnectorType}.";
                        }
                    }

                    var skipReason = Skip;
                    var aiIntegrationConfiguration = string.IsNullOrEmpty(skipReason)
                        ? aiConnectionStringForTesting.GetAiConfiguration()
                        : null;

                    var effectiveOptions = ModifyOptionsForConnector(aiConnectionStringForTesting, options);

                    var row = Data == null || Data.Length == 0
                        ? new TheoryDataRow(effectiveOptions, aiIntegrationConfiguration)
                        : new TheoryDataRow(new object[] { effectiveOptions, aiIntegrationConfiguration }.Concat(Data).ToArray());

                    if (string.IsNullOrEmpty(skipReason) == false)
                        row.Skip = skipReason;

                    result.Add(row);
                }
            }
        }
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
    }

    private DisposableAction ResetSkipReason(string skip) => new(() => Skip = skip);

    protected virtual RavenTestBase.Options ModifyOptionsForConnector(IAiConnectorForTesting<TConfig> connector, RavenTestBase.Options options) => options;

    private bool HasSkipReason(IAiConnectorForTesting<TConfig> aiConnectorForTesting)
    {
        if (string.IsNullOrEmpty(Skip) == false)
            return true;

        if (RavenTestHelper.EnvironmentVariables.SkipAiIntegrationTests)
        {
            Skip = RavenTestHelper.SkipAiIntegrationMessage;
            return true;
        }

        if (Is32Bit)
        {
            Skip = "AI tests are skipped on 32-bit process";
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
            return false;

        if (aiConnectorForTesting.MissingRequiredEnvVariables(out var envVar))
        {
            Skip = $"The environment variable {envVar} is required for {aiConnectorForTesting.AiConnectorType}, but was not set.";
            return true;
        }

        return false;
    }

    public abstract IEnumerable<IAiConnectorForTesting<TConfig>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration);
}

public class RavenGenAiDataAttribute : AbstractRavenAiIntegrationDataAttribute<GenAiConfiguration>
{
    protected override RavenTestBase.Options ModifyOptionsForConnector(IAiConnectorForTesting<GenAiConfiguration> connector, RavenTestBase.Options options)
    {
        if (connector.AiConnectorType != AiConnectorType.Ollama)
            return options;

        // A local reasoning model needs more than the default 60s per completion.
        options = options.Clone();
        options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Ai.GenAiSendToModelTimeout)] = "180";
        return options;
    }

    public static IEnumerable<IAiConnectorForTesting<GenAiConfiguration>> GetAiConnectionStrings(RavenAiIntegration aiIntegration)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return GenAiOpenAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return GenAiOllamaConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return GenAiAzureOpenAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.vLLM))
            yield return GenAiVllmConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Google))
            yield return GenAiGoogleConnectorForTesting.Instance;
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

        if (aiIntegration.HasFlag(RavenAiIntegration.vLLM))
            yield return EmbeddingsVllmConnectorForTesting.Instance;
    }

    public override IEnumerable<IAiConnectorForTesting<EmbeddingsGenerationConfiguration>> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration) => GetAiConnectionStrings(aiIntegration);
}
