using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : RequestHandler
    {
        [RavenAction("/databases/*/replication/topology", "GET")]
        public Task GetReplicationTopology()
        {
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }
    }
}
