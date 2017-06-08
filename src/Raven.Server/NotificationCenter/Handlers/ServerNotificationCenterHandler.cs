using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Util;
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
                    IEnumerable<NotificationTableValue> storedNotifications;

                    using (ServerStore.NotificationCenter.GetStored(out storedNotifications, postponed: false))
                    {
                        foreach (var action in storedNotifications)
                        {
                            await writer.WriteToWebSocket(action.Json);
                        }
                    }

                    await writer.WriteNotifications();
                }
            }
        }

        [RavenAction("/notification-center/dismiss", "POST")]
        public Task DismissPost()
        {
            var id = GetStringQueryString("id");

            var forever = GetBoolValueQueryString("forever", required: false);

            if (forever == true)
                ServerStore.NotificationCenter.Postpone(id, DateTime.MaxValue);
            else
                ServerStore.NotificationCenter.Dismiss(id);

            return NoContent();
        }

        [RavenAction("/notification-center/postpone", "POST")]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            ServerStore.NotificationCenter.Postpone(id, SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec)));
            
            return NoContent();
        }
    }
}