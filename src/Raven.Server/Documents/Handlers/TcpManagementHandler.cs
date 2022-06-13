using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Tcp;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class TcpManagementHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/tcp", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetAll()
        {
            using (var processor = new TcpManagementHandlerProcessorForGetAll(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/tcp", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new TcpManagementHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
