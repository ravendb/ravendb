using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Dashboard;
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
                    var serverInfo = new ServerInfo
                    {
                        StartUpTime = ServerStore.Server.Statistics.StartUpTime
                    };
                    await writer.WriteToWebSocket(serverInfo.ToJson());

                    using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
                    {
                        var databasesInfo = DatabasesInfoNotificationSender.FetchDatabasesInfo(ServerStore, cts);
                        foreach (var info in databasesInfo)
                        {
                            await writer.WriteToWebSocket(info.ToJson());
                        }
                    }

                    var machineResources = MachineResourcesNotificationSender.GetMachineResources();
                    await writer.WriteToWebSocket(machineResources.ToJson());

                    await writer.WriteNotifications();
                }
            }
        }
    }
}
