using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studo
{
    public class StudioServerConfigHandler : RequestHandler
    {
        [RavenAction("/studio-tasks/server-configs", "GET")]
        public Task Get()
        {
            //TODO: implement
            return HttpContext.Response.WriteAsync("{\"IsGlobalAdmin\":true,\"CanReadWriteSettings\":true,\"CanReadSettings\":true,\"CanExposeConfigOverTheWire\":true}");
        }
    }
}