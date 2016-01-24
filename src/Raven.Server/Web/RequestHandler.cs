using System.Threading.Tasks;
using Microsoft.AspNet.Http;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        public virtual Task Get(HttpContext ctx)
        {
            return Task.CompletedTask;
        }

        public virtual Task Put(HttpContext ctx)
        {
            return Task.CompletedTask;
        }
    }
}