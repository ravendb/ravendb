using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
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
                using (var writer = new NotificationCenterWebsocketWriter(webSocket, ServerStore.NotificationCenter, ServerStore.ContextPool, ServerStore.ServerShutdown))
                {
                    IEnumerable<ActionTableValue> storedActions;

                    using (ServerStore.NotificationCenter.GetStored(out storedActions, postponed: false))
                    {
                        foreach (var action in storedActions)
                        {
                            await writer.WriteToWebSocket(action.Json);
                        }
                    }

                    await writer.WriteNotifications();
                }
            }
        }
    }
}