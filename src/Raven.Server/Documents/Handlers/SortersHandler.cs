using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Sorters;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class SortersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/sorters", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new SortersHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }
    }
}
