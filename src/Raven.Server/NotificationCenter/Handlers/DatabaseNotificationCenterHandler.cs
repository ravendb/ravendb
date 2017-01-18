using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Actions.Database;
using Raven.Server.Routing;

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
                    await writer.WriteNotifications();
                }
            }
        }
    }
}