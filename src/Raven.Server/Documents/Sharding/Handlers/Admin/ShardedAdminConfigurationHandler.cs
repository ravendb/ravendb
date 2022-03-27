using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Configuration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    internal class ShardedAdminConfigurationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/configuration/settings", "GET")]
        public async Task GetSettings()
        {
            using (var processor = new ShardedAdminConfigurationHandlerProcessorForGetSettings(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/record", "GET")]
        public async Task GetDatabaseRecord()
        {
            using (var processor = new ShardedAdminConfigurationHandlerProcessorForGetDatabaseRecord(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/configuration/settings", "PUT")]
        public async Task PutSettings()
        {
            using (var processor = new ShardedAdminConfigurationHandlerProcessorForPutSettings(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/configuration/studio", "PUT")]
        public async Task GetStudioConfiguration()
        {
            using (var processor = new ShardedAdminConfigurationHandlerProcessorForPutStudioConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
