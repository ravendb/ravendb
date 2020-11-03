using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Dashboard;
using Raven.Server.Routing;
using Sparrow.Logging;
using Sparrow.Server.LowMemory;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ServerDashboardHandler : ServerNotificationHandlerBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerDashboardHandler>("Server");

        [RavenAction("/server-dashboard/watch", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task Get()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var writer = new NotificationCenterWebSocketWriter(webSocket, ServerStore.ServerDashboardNotifications, ServerStore.ContextPool,
                    ServerStore.ServerShutdown))
                {
                    var isValidFor = GetDatabaseAccessValidationFunc();
                    try
                    {
                        using (var lowMemoryMonitor = new LowMemoryMonitor())
                        {
                            var machineResources = MachineResourcesNotificationSender.GetMachineResources(Server.MetricCacher, lowMemoryMonitor, Server.CpuUsageCalculator);
                            await writer.WriteToWebSocket(machineResources.ToJson());
                        }

                        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
                        {
                            var databasesInfo = new List<AbstractDashboardNotification>();

                            foreach (var item in DatabasesInfoNotificationSender.FetchDatabasesInfo(ServerStore, isValidFor, cts))
                            {
                                databasesInfo.Add(item);
                            }

                            foreach (var info in databasesInfo)
                            {
                                await writer.WriteToWebSocket(info.ToJson());
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failed to send the initial server dashboard data", e);
                    }

                    await writer.WriteNotifications(isValidFor);
                }
            }
        }
    }
}
