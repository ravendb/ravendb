using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions;
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

        [RavenAction("/notification-center/dismiss", "POST")]
        public Task DismissPost()
        {
            var actionId = GetStringQueryString("id");

            ServerStore.NotificationCenter.Dismiss(actionId);

            return Task.CompletedTask;
        }

        [RavenAction("/notification-center/postpone", "POST")]
        public Task PostponePost()
        {
            var actionId = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            ServerStore.NotificationCenter.Postpone(actionId, SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec.Value)));

            return Task.CompletedTask;
        }
    }
}