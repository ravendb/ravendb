using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class OpenAiConnectorForTesting : BaseAiConnectorForTesting<OpenAiConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_OPENAI_API_KEY";
    private const string Endpoint = "https://api.openai.com/v1";
    private const string Model = "text-embedding-3-small";

    public OpenAiConnectorForTesting()
    {
        RequiredEnvironmentVariables = [EnvironmentVariable];
    }
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.OpenAi);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);

        return new AiConnectionString
        {
            OpenAiSettings = new OpenAiSettings(apiKey, Endpoint, Model)
        };
    }
}
