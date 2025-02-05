using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiEtlHandlerProcessorForTestGoogleConnection<TRequestHandler, TOperationContext> : AiEtlHandlerProcessorForTestAiConnectionBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public AiEtlHandlerProcessorForTestGoogleConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override (AiConnectorType, AiConnectionString) GetAiConnectorDetails()
    {
        var googleSettings = JsonConvert.DeserializeObject<GoogleSettings>(JsonConfigString);
        return (AiConnectorType.Google, new AiConnectionString { GoogleSettings = googleSettings });
    }
}
