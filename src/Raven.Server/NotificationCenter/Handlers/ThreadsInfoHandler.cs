using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Dashboard;
using Raven.Server.Routing;
using Raven.Server.Utils;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ThreadsInfoHandler : ServerNotificationCenterHandler
    {
        [RavenAction("/threads-info/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task GetThreadsInfo()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (var writer  = new NotificationCenterWebSocketWriter(webSocket, ServerStore.ThreadsInfoNotifications, ServerStore.ContextPool, ServerStore.ServerShutdown))
            {
                await writer.WriteNotifications(null);
            }
        }
    }
}
