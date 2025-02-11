using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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

    All = OpenAi | AzureOpenAI | Ollama | Onnx | Google | HuggingFace | MistralAi,
    NonInternal = OpenAi | AzureOpenAI | Ollama | Google | HuggingFace | MistralAi
}

public  class RavenAiIntegrationDataAttribute : RavenDataAttributeBase
{
    public RavenDatabaseMode DatabaseMode { get; set; } = RavenDatabaseMode.All;
    public RavenAiIntegration IntegrationType { get; set; } = RavenAiIntegration.All;
    public object[] Data { get; set; } = null;
    public bool NightlyBuildRequired { get; set; } = true;
    public bool CheckCanConnect { get; set; } = true;
    public bool ReuseConnectionString { get; set; } = true;

    public RavenAiIntegrationDataAttribute()
    {
    }

    public RavenAiIntegrationDataAttribute(params object[] data) : this()
    {
        Data = data;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        var skipMessageCopy = Skip;
        foreach (var (databaseMode, options) in RavenDataAttribute.GetOptions(DatabaseMode))
        {
            Func<IEnumerable<IAiConnectorForTesting>> aiConnectionStringForTestingGetter = ReuseConnectionString
                ? () => GetAiConnectionStringsSingleton(IntegrationType)
                : () => GetAiConnectionStringsNewInstance(IntegrationType, testMethod.Name);

            foreach (var aiConnectionStringForTesting in aiConnectionStringForTestingGetter.Invoke())
            {
                using (SetSkipValueIfSkipMessageNotEmpty(ref skipMessageCopy))
                using (SetSkipValueIfNightlyBuildRequired(ref skipMessageCopy))
                using (SetSkipValueIfShardedDbOnX86(databaseMode, ref skipMessageCopy))
                using (SetSkipValueIfUnableConnectToAi(aiConnectionStringForTesting, skipMessageCopy))
                {
                    var aiEtlConfiguration = aiConnectionStringForTesting.GetEtlConfiguration();

                    if (Data == null || Data.Length == 0)
                    {
                        yield return [options, aiEtlConfiguration];
                        continue;
                    }

                    yield return new object[] { options, aiEtlConfiguration }.Concat(Data).ToArray();
                }
            }
        }
    }

    private DisposableAction SetSkipValueIfSkipMessageNotEmpty(ref string skipMessage)
    {
        if (string.IsNullOrEmpty(skipMessage))
            return null;

        Skip = skipMessage;
        return new DisposableAction(() => Skip = null);
    }

    private DisposableAction SetSkipValueIfShardedDbOnX86(RavenDatabaseMode databaseMode, ref string skipMessage)
    {
        if (string.IsNullOrEmpty(skipMessage) == false)
            return null;

        if (Is32Bit == false)
            return null;

        if (databaseMode.HasFlag(RavenDatabaseMode.Sharded) == false)
            return null;

        Skip = ShardingSkipMessage;
        return new DisposableAction(() => Skip = null);
    }

    private DisposableAction SetSkipValueIfUnableConnectToAi(IAiConnectorForTesting aiConnectorForTesting, string skipMessage)
    {
        if (string.IsNullOrEmpty(skipMessage) == false)
            return null;

        if (CheckCanConnect == false)
            return null;

        if (CanConnectToAi(aiConnectorForTesting, out string unableToConnectMessage))
            return null;

        Skip = unableToConnectMessage;
        return new DisposableAction(() => Skip = null);
    }

    private bool CanConnectToAi(IAiConnectorForTesting aiConnectorForTesting, out string skipMessage)
    {
        if (aiConnectorForTesting.CanConnect.Value)
        {
            skipMessage = Skip;
            return true;
        }

        skipMessage = $"Test requires connection to {aiConnectorForTesting.AiConnectorType.Value}.";
        return false;
    }

    private DisposableAction SetSkipValueIfNightlyBuildRequired(ref string skipMessage)
    {
        if (string.IsNullOrEmpty(skipMessage) == false)
            return null;

        if (NightlyBuildRequired == false || NightlyBuildTheoryAttribute.IsNightlyBuild)
            return null;

        Skip = NightlyBuildTheoryAttribute.SkipMessage;
        return new DisposableAction(() => Skip = null);
    }

    private static IEnumerable<IAiConnectorForTesting> GetAiConnectionStringsNewInstance(RavenAiIntegration aiIntegration, string testMethodName)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return OpenAiConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return AzureOpenAiConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return OllamaConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.Onnx))
            yield return OnnxConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.Google))
            yield return GoogleConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.HuggingFace))
            yield return HuggingFaceConnectorForTesting.CreateNewInstance(testMethodName);

        if (aiIntegration.HasFlag(RavenAiIntegration.MistralAi))
            yield return MistralAiConnectorForTesting.CreateNewInstance(testMethodName);
    }

    private static IEnumerable<IAiConnectorForTesting> GetAiConnectionStringsSingleton(RavenAiIntegration aiIntegration)
    {
        if (aiIntegration.HasFlag(RavenAiIntegration.OpenAi))
            yield return OpenAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.AzureOpenAI))
            yield return AzureOpenAiConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Ollama))
            yield return OllamaConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Onnx))
            yield return OnnxConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.Google))
            yield return GoogleConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.HuggingFace))
            yield return HuggingFaceConnectorForTesting.Instance;

        if (aiIntegration.HasFlag(RavenAiIntegration.MistralAi))
            yield return MistralAiConnectorForTesting.Instance;
    }
}
