using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedDocumentHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
        }
    }
}