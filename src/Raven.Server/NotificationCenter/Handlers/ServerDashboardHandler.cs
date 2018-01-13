using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Dashboard;
using Raven.Server.Routing;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerDashboardHandler : ServerNotificationHandlerBase
    {
        [RavenAction("/server-dashboard/watch", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebSocketWriter(webSocket, ServerStore.ServerDashboardNotifications, ServerStore.ContextPool, ServerStore.ServerShutdown))
                {
                    var isValidFor = GetDatabaseAccessValidationFunc();
                    var machineResources = MachineResourcesNotificationSender.GetMachineResources();
                    await writer.WriteToWebSocket(machineResources.ToJson());

                    using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
                    {
                        var databasesInfo = DatabasesInfoNotificationSender.FetchDatabasesInfo(ServerStore, isValidFor, cts);
                        foreach (var info in databasesInfo)
                        {
                            await writer.WriteToWebSocket(info.ToJson());
                        }
                    }

                    await writer.WriteNotifications(isValidFor);
                }
            }
        }
    }
}
