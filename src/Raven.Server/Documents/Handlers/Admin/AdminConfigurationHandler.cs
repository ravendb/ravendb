using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/configuration/settings", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public async Task GetSettings()
        {
            using (var processor = new AdminConfigurationHandlerProcessorForGetSettings(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/record", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetDatabaseRecord()
        {
            using (var processor = new AdminConfigurationHandlerForGetDatabaseRecord(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/configuration/settings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutSettings()
        {
            using (var processor = new AdminConfigurationHandlerProcessorForPutSettings(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/configuration/studio", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutStudioConfiguration()
        {
            using (var processor = new AdminConfigurationHandlerProcessorForPutStudioConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/configuration/client", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutClientConfiguration()
        {
            using (var processor = new AdminConfigurationHandlerProcessorForPutClientConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
