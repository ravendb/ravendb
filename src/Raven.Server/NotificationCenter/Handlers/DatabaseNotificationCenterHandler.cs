using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Actions;
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
                using (var writer = new NotificationCenterWebsocketWriter(webSocket, Database.NotificationCenter, ContextPool, Database.DatabaseShutdown))
                {
                    IEnumerable<ActionTableValue> storedActions;

                    using (Database.NotificationCenter.GetStored(out storedActions, postponed: false))
                    {
                        foreach (var alert in storedActions)
                        {
                            await writer.WriteToWebSocket(alert.Json);
                        }
                    }

                    foreach (var operation in Database.Operations.GetActive().OrderBy(x => x.Description.StartTime))
                    {
                        var action = OperationChanged.Create(operation.Id, operation.Description, operation.State);

                        await writer.WriteToWebSocket(action.ToJson());
                    }
                    
                    await writer.WriteNotifications();
                }
            }
        }
    }
}