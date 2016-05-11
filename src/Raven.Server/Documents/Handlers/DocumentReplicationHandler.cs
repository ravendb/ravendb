using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/documentReplication", "GET", "/databases/{databaseName:string}/documentReplication")]
        public async Task DocumentReplicationConnection()
        {
            DocumentsOperationContext context;
            
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (ContextPool.AllocateOperationContext(out context))
            {
                //TODO : add disconnect at heartbeat timeout -> stop "while" loop at timeout
                while (!Database.DatabaseShutdown.IsCancellationRequested)
                {					
                }
            }
        }
    }
}
