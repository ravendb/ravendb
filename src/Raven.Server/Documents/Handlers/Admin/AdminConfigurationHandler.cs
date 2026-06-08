using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Documents.Handlers.Admin
{
    public sealed class AdminConfigurationHandler : DatabaseRequestHandler
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
            Database.ForTestingPurposes?.DatabaseRecordLoadHold?.WaitOne();

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

        [RavenAction("/databases/*/admin/features/pull-replication-composite-change-vectors", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task SetPullReplicationCompositeChangeVectorsFeature()
        {
            bool enabled = GetBoolValueQueryString("enabled", required: true)!.Value;
            (long index, _) = await ServerStore.SendToLeaderAsync(new SetPullReplicationCompositeChangeVectorsFeatureCommand(DatabaseName, enabled, GetRaftRequestIdFromQuery())).ConfigureAwait(false);
            await WaitForIndexNotificationAsync(index).ConfigureAwait(false);
            NoContentStatus();
        }
    }
}
