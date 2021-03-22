using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerWideQueriesDebugHandler : RequestHandler
    {
        [RavenAction("/debug/queries/running/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task RunningQueriesLive()
        {
            var allowedDbs = await GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

            if (allowedDbs.HasAccess == false)
                return;

            HashSet<string> dbNames = null;
            if (allowedDbs.AuthorizedDatabases != null)
            {
                dbNames = allowedDbs.AuthorizedDatabases.Keys.ToHashSet();
            }

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, ServerStore.ServerShutdown);

                await using (var ms = new MemoryStream())
                using (var collector = new LiveRunningQueriesCollector(ServerStore, dbNames))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ServerStore.ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
                    while (ServerStore.ServerShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ServerStore.ContextPool, ms, 3000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
}
