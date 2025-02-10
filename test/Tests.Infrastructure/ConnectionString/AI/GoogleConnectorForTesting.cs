using System;
using Raven.Client.Documents.Operations.ETL.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class GoogleConnectorForTesting : BaseAiConnectorForTesting<GoogleConnectorForTesting>
{
    private const string EnvironmentVariable = "RAVEN_AI_INTEGRATION_GOOGLE_API_KEY";
    private const string Model = "text-embedding-004";

    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.ETL.AI.AiConnectorType.Google);

    protected override AiConnectionString CreateAiConnectionStringImpl()
    {
        var apiKey = Environment.GetEnvironmentVariable(EnvironmentVariable);
        return new AiConnectionString
        {
            GoogleSettings = new GoogleSettings(Model, apiKey)
        };
    }
}
