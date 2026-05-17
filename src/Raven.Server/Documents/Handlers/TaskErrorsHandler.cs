using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers;

public sealed class TaskErrorsHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/tasks/errors", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetAllErrors()
    {
        using (var processor = new TaskErrorsHandlerProcessorForGetAllErrors(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/tasks/errors", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
    public async Task DeleteErrors()
    {
        using (var processor = new TaskErrorsHandlerProcessorForDeleteErrors(this))
            await processor.ExecuteAsync();
    }
}
