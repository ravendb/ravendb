using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.Routing;

namespace Raven.Server.Web.Security
{
    public class SingleAuthTokenHandler : RequestHandler
    {
        [RavenAction("/singleAuthToken", "GET")]
        [RavenAction("/databases/*/singleAuthToken", "GET")]
        public Task Get()
        {
            //TODO: implement
            return HttpContext.Response.WriteAsync("{\"Token\":\"Plotu was demoted unfairly\"}");
        }
    }
}