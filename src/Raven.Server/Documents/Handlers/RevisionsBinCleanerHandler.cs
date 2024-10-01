using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers;
public class RevisionsBinCleanerHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/revisions/bin-cleaner/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetRevisionsBinConfig()
    {
        using (var processor = new RevisionsBinCleanerHandlerProcessorForGetConfiguration(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/revisions/bin-cleaner/config", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task ConfigRevisionsBinCleaner()
    {
        using (var processor = new RevisionsBinCleanerHandlerProcessorForPostConfiguration(this))
            await processor.ExecuteAsync();
    }
}

