using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class HuggingFaceConnectorForTesting : BaseAiConnectorForTesting<HuggingFaceConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_HUGGINGFACE_API_KEY";
    private const string Model = "sentence-transformers/all-MiniLM-L6-v2";

    public HuggingFaceConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.HuggingFace);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            HuggingFaceSettings = new HuggingFaceSettings(apiKey, Model)
        };
    }
}
