using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.NotificationCenter.Actions.Server;
using Raven.Server.Routing;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerNotificationCenterHandler : AdminRequestHandler
    {
        [RavenAction("/notification-center/watch", "GET")]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebsocketWriter<ServerAction>(webSocket, ServerStore.NotificationCenter, ServerStore.ContextPool, ServerStore.ServerShutdown))
                {
                    await writer.WriteNotifications();
                }
            }
        }
    }
}