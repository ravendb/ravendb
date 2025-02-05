using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiEtlHandlerProcessorForTestHuggingFaceConnection<TRequestHandler, TOperationContext> : AiEtlHandlerProcessorForTestAiConnectionBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public AiEtlHandlerProcessorForTestHuggingFaceConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override (AiConnectorType, AiConnectionString) GetAiConnectorDetails()
    {
        var huggingFaceSettings = JsonConvert.DeserializeObject<HuggingFaceSettings>(JsonConfigString);
        return (AiConnectorType.HuggingFace, new AiConnectionString { HuggingFaceSettings = huggingFaceSettings });
    }
}
