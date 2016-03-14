using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Routing;

namespace Raven.Server.Web.TEMP_REMOVE_ME
{
    public class MakeStudioWorkForNowHandler : RequestHandler
    {
        [RavenAction("/queries", "POST")]
        public Task FakeResponseForQueriesPost()
        {
            return HttpContext.Response.WriteAsync("{'Results':[],'Includes':[]}");
        }

        [RavenAction("/replication/topology", "GET")]
        [RavenAction("/databases/*/replication/topology", "GET")]
        public Task FakeResponseForReplicationTopology()
        {
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/configuration/document$", "GET")]
        public Task FakeResponseForConfigurationDocument()
        {
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }

        [RavenAction("/studio-tasks/latest-server-build-version", "GET")]
        public Task FakeResponseForLastServerBuild()
        {
            return HttpContext.Response.WriteAsync("{'LatestBuild':'4.0.404'}");
        }

        [RavenAction("/license/status", "GET", IgnoreDbRoute = true)]
        public Task FakeResponseForLicenseStatus()
        {
            return HttpContext.Response.WriteAsync("{'Status':'AGPL','Error':false,'Attributes':{}, 'Message': 'Hi there'}");
        }
    }
}