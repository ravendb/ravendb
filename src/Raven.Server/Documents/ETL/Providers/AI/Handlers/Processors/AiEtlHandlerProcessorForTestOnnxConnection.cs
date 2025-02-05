using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiEtlHandlerProcessorForTestOnnxConnection<TRequestHandler, TOperationContext> : AiEtlHandlerProcessorForTestAiConnectionBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public AiEtlHandlerProcessorForTestOnnxConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override (AiConnectorType, AiConnectionString) GetAiConnectorDetails()
    {
        var onnxSettings = JsonConvert.DeserializeObject<OnnxSettings>(JsonConfigString);
        return (AiConnectorType.Onnx, new AiConnectionString { OnnxSettings = onnxSettings });
    }
}
