using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/configuration/client", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutClientConfiguration()
        {
            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var clientConfigurationJson = context.ReadForDisk(RequestBodyStream(), Constants.Configuration.ClientId);
                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);

                using (context.OpenReadTransaction())
                {
                    var record = ServerStore.Cluster.ReadDatabase(context, Database.Name, out long index);
                    record.Client = clientConfiguration;
                    record.Client.Etag++; // we don't care _what_ the value is, just that it is changing

                    var result = await ServerStore.WriteDatabaseRecordAsync(Database.Name, record, index);
                    await Database.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, ServerStore.Engine.OperationTimeout);
                }

                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }
    }
}
