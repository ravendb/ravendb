using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.Handlers
{
    public sealed class ThreadsInfoHandler : ServerNotificationHandlerBase
    {
        [RavenAction("/threads-info/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task GetThreadsInfo()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (var writer = new NotificationCenterWebSocketWriter<TransactionOperationContext>(webSocket, ServerStore.ThreadsInfoNotifications, ServerStore.ContextPool, ServerStore.ServerShutdown))
            {
                await writer.WriteNotifications(null);
            }
        }
    }
}
