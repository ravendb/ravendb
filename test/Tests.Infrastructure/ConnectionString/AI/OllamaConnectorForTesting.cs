using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class OllamaConnectorForTesting : BaseAiConnectorForTesting<OllamaConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_HUGGINGFACE_URI";
    private const string Model = "mistral-nemo:latest";

    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Ollama);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var uri = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            OllamaSettings = new OllamaSettings(uri, Model)
        };
    }
}
