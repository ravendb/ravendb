using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiEtlHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/etl/ai/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (var processor = new AiEtlHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
