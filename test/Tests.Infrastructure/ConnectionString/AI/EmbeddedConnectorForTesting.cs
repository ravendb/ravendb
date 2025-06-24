using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddedConnectorForTesting : AbstractEmbeddingsConnectorForTesting<EmbeddedConnectorForTesting>
{
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Embedded);

    protected override AiConnectionString CreateAiConnectionStringImpl() => new() { EmbeddedSettings = new EmbeddedSettings(), ModelType = AiModelType.TextEmbeddings };
}
