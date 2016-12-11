using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminLogsHandler : AdminRequestHandler
    {
        [RavenAction("/admin/logs/watch", "GET", "/admin/logs/watch")]
        public async Task RegisterForLogs()
        {
            string db = HttpContext.Request.Query["db"];
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                await LoggingSource.Instance.Register(socket,db);
            }
        }

    }
}