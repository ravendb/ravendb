using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerNotificationCenterHandler : RequestHandler
    {
        [RavenAction("/admin/notification-center/watch", "GET", AuthorizationStatus.Operator)]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebSocketWriter(webSocket, ServerStore.NotificationCenter, ServerStore.ContextPool, ServerStore.ServerShutdown))
                {
                    using (ServerStore.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
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

        [RavenAction("/admin/notification-center/dismiss", "POST", AuthorizationStatus.Operator)]
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

        [RavenAction("/admin/notification-center/postpone", "POST", AuthorizationStatus.Operator)]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            ServerStore.NotificationCenter.Postpone(id, SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec)));
            
            return NoContent();
        }
    }
}
