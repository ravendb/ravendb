using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Actions.Database;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/notification-center/watch", "GET")]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebsocketWriter<DatabaseAction>(webSocket, Database.NotificationCenter, ContextPool, Database.DatabaseShutdown))
                {
                    IEnumerable<BlittableJsonReaderObject> storedActions;

                    using (Database.NotificationCenter.GetStored(out storedActions))
                    {
                        foreach (var alert in storedActions)
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