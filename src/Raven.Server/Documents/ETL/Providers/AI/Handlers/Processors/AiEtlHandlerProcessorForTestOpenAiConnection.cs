using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal sealed class AiEtlHandlerProcessorForTestOpenAiConnection<TRequestHandler, TOperationContext> : AiEtlHandlerProcessorForTestAiConnectionBase<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public AiEtlHandlerProcessorForTestOpenAiConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override (AiConnectorType, AiConnectionString) GetAiConnectorDetails()
    {
        var openAiSettings = JsonConvert.DeserializeObject<OpenAiSettings>(JsonConfigString);
        return (AiConnectorType.OpenAi, new AiConnectionString { OpenAiSettings = openAiSettings });
    }
}
