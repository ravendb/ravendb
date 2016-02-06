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

        [RavenAction("/studio-tasks/latest-server-build-version", "GET")]
        public Task FakeResponseForLastServerBuild()
        {
            return HttpContext.Response.WriteAsync("{'LatestBuild':'4.0.404'}");
        }

        [RavenAction("/license/status", "GET")]
        public Task FakeResponseForLicenseStatus()
        {
            return HttpContext.Response.WriteAsync("{'Status':'AGPL','Error':false,'Attributes':{}, 'Message': 'Hi there'}");
        }
    }
}