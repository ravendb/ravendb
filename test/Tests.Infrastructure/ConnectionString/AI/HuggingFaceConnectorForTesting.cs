using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddingsHuggingFaceConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddingsHuggingFaceConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_HUGGINGFACE_API_KEY";
    private const string Model = "sentence-transformers/all-MiniLM-L6-v2";

    public EmbeddingsHuggingFaceConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.HuggingFace;

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            ModelType = AiModelType.TextEmbeddings,
            HuggingFaceSettings = new HuggingFaceSettings(apiKey, Model)
        };
    }
}
