using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiEtlConnectionHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/etl/ai/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestAiConnection()
    {
        var aiConnectorTypeString = GetStringQueryString("type");

        if (Enum.TryParse(aiConnectorTypeString, out AiConnectorType aiConnectorType) == false)
            throw new ArgumentException($"Invalid AI connector type: '{aiConnectorTypeString}'");

        if (aiConnectorType == AiConnectorType.None)
            throw new ArgumentException("AI connector type cannot be 'None'");

        using (var processor = new AiEtlHandlerProcessorForTestAiConnection<DatabaseRequestHandler, DocumentsOperationContext>(this)
               { AiConnectorType = aiConnectorType })
            await processor.ExecuteAsync();
    }
}
