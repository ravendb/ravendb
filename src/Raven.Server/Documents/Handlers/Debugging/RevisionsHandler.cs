using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class RevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/get-revisions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisions()
        {
            using (var processor = new RevisionsHandlerProcessorForGetRevisionsDebug(this))
                await processor.ExecuteAsync();
        }
    }
}
