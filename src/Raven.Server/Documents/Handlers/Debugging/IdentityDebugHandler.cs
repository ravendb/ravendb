using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Identities;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class IdentityDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/identities", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetIdentities()
        {
            using (var processor = new IdentityDebugHandlerProcessorForGetIdentities(this))
                await processor.ExecuteAsync();
        }
    }
}
