using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.NotificationCenter.Actions.Server;
using Raven.Server.Routing;
using Sparrow.Json;

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
                    IEnumerable<BlittableJsonReaderObject> existingAlerts;

                    using (ServerStore.NotificationCenter.GetAlerts(out existingAlerts))
                    {
                        foreach (var alert in existingAlerts)
                        {
                            await writer.WriteToWebSocket(alert);
                        }
                    }

                    await writer.WriteNotifications();
                }
            }
        }
    }
}