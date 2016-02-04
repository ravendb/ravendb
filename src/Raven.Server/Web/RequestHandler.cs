using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RequestHandler).FullName);

        protected HttpContext HttpContext;
        public ServerStore ServerStore;
        public RouteMatch RouteMatch;

        public virtual void Init(RequestHandlerContext context)
        {
            HttpContext = context.HttpContext;
            ServerStore = context.ServerStore;
            RouteMatch = context.RouteMatch;
        }

        protected Stream RequestBodyStream()
        {
            var requestBodyStream = HttpContext.Request.Body;

            var contentEncoding = HttpContext.Request.Headers["Content-Encoding"];
            if (contentEncoding != "gzip")
                return requestBodyStream;

            var gZipStream = new GZipStream(requestBodyStream, CompressionMode.Decompress);
            return gZipStream;
        }

        protected Stream ResponseBodyStream()
        {
               var responseBodyStream = HttpContext.Response.Body;

            var contentEncoding = HttpContext.Response.Headers["Content-Encoding"];
            if (contentEncoding != "")
                return responseBodyStream;

            HttpContext.Response.Headers["Content-Encoding"] = "gzip";
            var gZipStream = new GZipStream(responseBodyStream, CompressionMode.Compress);
            return gZipStream;
        }
    }
}