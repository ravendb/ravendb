using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class ConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/configuration/studio", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetStudioConfiguration()
        {
            using (var processor = new ConfigurationHandlerProcessorForGetStudioConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/configuration/client", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetClientConfiguration()
        {
            using (var processor = new ConfigurationHandlerProcessorForGetClientConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
