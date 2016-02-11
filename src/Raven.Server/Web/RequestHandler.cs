using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(RequestHandler).FullName);

        protected HttpContext HttpContext;
        public RavenServer Server;
        public ServerStore ServerStore;
        public RouteMatch RouteMatch;

        public virtual void Init(RequestHandlerContext context)
        {
            HttpContext = context.HttpContext;
            Server = context.RavenServer;
            ServerStore = Server.ServerStore;
            RouteMatch = context.RouteMatch;
        }

        protected Stream RequestBodyStream()
        {
            var requestBodyStream = HttpContext.Request.Body;

            if(IsGzipRequest()==false)
                return  requestBodyStream;

            var gZipStream = new GZipStream(requestBodyStream, CompressionMode.Decompress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        private bool IsGzipRequest()
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in HttpContext.Request.Headers["Content-Encoding"])
            {
                if (val == "gzip")
                    return true;
            }
            return false;
        }

        protected Stream ResponseBodyStream()
        {
            var responseBodyStream = HttpContext.Response.Body;

            if (CanAcceptGzip() == false)
                return responseBodyStream;

            HttpContext.Response.Headers["Content-Encoding"] = "gzip";
            var gZipStream = new GZipStream(responseBodyStream, CompressionMode.Compress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        private bool CanAcceptGzip()
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in HttpContext.Request.Headers["Accept-Encoding"])
            {
                if (val == "gzip")
                    return true;
            }
            return false;
        }

        protected long? GetLongFromHeaders(string name)
        {
            var etags = HttpContext.Request.Headers[name];
            if (etags.Count == 0)
                return null;

            long etag;
            if (long.TryParse(etags[0], out etag) == false)
                throw new ArgumentException(
                    "Could not parse header '" + name + "' header as int64, value was: " + etags[0]);
            return etag;
        }

        protected int GetStart(int defaultValue = 0)
        {
            return GetIntQueryString("start", defaultValue);
        }

        protected int GetPageSize(int defaultValue = 25)
        {
            return GetIntQueryString("pageSize", defaultValue);
        }

        protected int GetIntQueryString(string name, int? defaultValue = null)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (defaultValue.HasValue)
                    return defaultValue.Value;
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");
            }

            int result;
            if (int.TryParse(val[0], out result) == false)
                throw new ArgumentException(
                    string.Format("Could not parse query string '{0}' header as int32, value was: {1}", name, val[0]));
            return result;
        }


        protected long GetLongQueryString(string name)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

            long result;
            if (long.TryParse(val[0], out result) == false)
                throw new ArgumentException(
                    string.Format("Could not parse query string '{0}' header as int64, value was: {1}", name, val[0]));
            return result;
        }

        protected string GetStringQueryString(string name)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

            return val[0];
        }

        protected StringValues GetStringValuesQueryString(string name)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
                throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

            return val;
        }
    }
}