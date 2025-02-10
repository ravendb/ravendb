using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiEtlHandlerProcessorForTestAzureOpenAiConnection<TRequestHandler, TOperationContext> : AiEtlHandlerProcessorForTestAiConnectionBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public AiEtlHandlerProcessorForTestAzureOpenAiConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override (AiConnectorType, AiConnectionString) GetAiConnectorDetails()
    {
        var azureOpenAiSettings = JsonConvert.DeserializeObject<AzureOpenAiSettings>(JsonConfigString);
        return (AiConnectorType.AzureOpenAi, new AiConnectionString { AzureOpenAiSettings = azureOpenAiSettings });
    }
}
