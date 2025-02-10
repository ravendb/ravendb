using System;
using Raven.Client.Documents.Operations.ETL.AI;

namespace Tests.Infrastructure.ConnectionString.AI;

public class OnnxConnectorForTesting : BaseAiConnectorForTesting<OnnxConnectorForTesting>
{
    public override Lazy<AiConnectorType> AiConnectorType { get; init; } = new(Raven.Client.Documents.Operations.ETL.AI.AiConnectorType.Onnx);

    protected override AiConnectionString CreateAiConnectionStringImpl() => new() { OnnxSettings = new OnnxSettings() };
}
