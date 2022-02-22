using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class RachisDatabaseHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/rachis/wait-for-raft-commands", "POST", AuthorizationStatus.Operator)]
        public async Task WaitForRaftCommands()
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
