using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class EmbeddedConnectorForTesting : BaseAiConnectorForTesting<EmbeddedConnectorForTesting>
{
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Embedded);

    protected override AiConnectionString CreateAiConnectionStringImpl() => new() { EmbeddedSettings = new EmbeddedSettings() };
}
