using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class RachisDatabaseHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/wait-for-indexes-notification", "POST", AuthorizationStatus.Operator)]
        public async Task WaitForIndexesNotification()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "raft-index-ids");
                var commands = JsonDeserializationServer.WaitForCommands(blittable);

                foreach (var index in commands.RaftIndexIds)
                {
                    await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, HttpContext.RequestAborted);
                }
            }
        }
    }
}
