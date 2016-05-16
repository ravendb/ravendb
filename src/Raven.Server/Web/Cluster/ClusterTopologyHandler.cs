using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.Routing;

namespace Raven.Server.Web.Cluster
{
    public class ClusterTopologyHandler : RequestHandler
    {
        [RavenAction("/cluster/topology", "GET")]
        public Task Get()
        {
            //TODO: Implement
            return HttpContext.Response.WriteAsync(@"{'AllVotingNodes':[]}");
        }
    }
}