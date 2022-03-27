using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

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
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var clientConfigurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), Constants.Configuration.ClientId);
                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);

                await UpdateDatabaseRecord(context, (record, index) =>
                {
                    record.Client = clientConfiguration;
                    record.Client.Etag = index;
                }, GetRaftRequestIdFromQuery());
            }

            NoContentStatus(HttpStatusCode.Created);
            HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";
        }

        private async Task UpdateDatabaseRecord(TransactionOperationContext context, Action<DatabaseRecord, long> action, string raftRequestId)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            using (context.OpenReadTransaction())
            {
                var record = ServerStore.Cluster.ReadDatabase(context, Database.Name, out long index);

                action(record, index);

                var result = await ServerStore.WriteDatabaseRecordAsync(Database.Name, record, index, raftRequestId);
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, ServerStore.Engine.OperationTimeout);
            }
        }
    }
}
