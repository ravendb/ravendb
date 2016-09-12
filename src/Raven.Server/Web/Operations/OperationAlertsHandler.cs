using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Routing;

namespace Raven.Server.Web.Operations
{
    public class OperationAlertsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/operation/alerts", "GET")]
        [RavenAction("/operation/alerts", "GET")]
        public Task Get()
        {
            //TODO: Implement
            return HttpContext.Response.WriteAsync(@"[]");
        }

    }
}