using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiTasksHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/ai/errors", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
    public async Task GetErrors()
    {
        using (var processor = new AiTasksHandlerProcessorForGetErrors(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/ai/errors", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
    public async Task DeleteErrors()
    {
        using (var processor = new AiTasksHandlerProcessorForDeleteErrors(this))
            await processor.ExecuteAsync();
    }
}
