using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddedConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddedConnectorForTesting>
{
    public override AiConnectorType AiConnectorType { get; init; } = AiConnectorType.Embedded;

    protected override AiConnectionString CreateAiConnectionStringImpl() => new() { EmbeddedSettings = new EmbeddedSettings(), ModelType = AiModelType.TextEmbeddings };
}
