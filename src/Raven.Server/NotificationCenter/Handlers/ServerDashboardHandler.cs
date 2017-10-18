using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerDashboardHandler : RequestHandler
    {
        [RavenAction("/server-dashboard/watch", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebSocketWriter(webSocket, ServerStore.ServerDashboardNotifications, ServerStore.ContextPool, ServerStore.ServerShutdown))
                {
                    await writer.WriteNotifications();
                }
            }
        }
    }
}
