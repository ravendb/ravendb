using System.Threading.Tasks;
using Raven.Client;

namespace Raven.Server.Web.System
{
    public sealed class CorsPreflightHandler : ServerRequestHandler
    {
        public Task HandlePreflightRequest()
        {
            // SetupCORSHeaders is called in generic handler - no need to call it here 
            
            HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
            
            return Task.CompletedTask;
        }
    }
}