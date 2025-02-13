using System;
using Raven.Client.Documents.Operations.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class OnnxConnectorForTesting : BaseAiConnectorForTesting<OnnxConnectorForTesting>
{
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.AI.AiConnectorType.Onnx);

    protected override AiConnectionString CreateAiConnectionStringImpl() => new() { OnnxSettings = new OnnxSettings() };
}
