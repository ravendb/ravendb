using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        public const string StartParameter = "start";

        public const string PageSizeParameter = "pageSize";

        protected static readonly ILog Log = LogManager.GetLogger(typeof(RequestHandler).FullName);

        private RequestHandlerContext _context;

        protected HttpContext HttpContext
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.HttpContext; }
        }

        public RavenServer Server
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RavenServer; }
        }
        public ServerStore ServerStore
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RavenServer.ServerStore; }
        }
        public RouteMatch RouteMatch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _context.RouteMatch; }
        }

        public virtual void Init(RequestHandlerContext context)
        {
            _context = context;
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

        protected bool IsWebsocketRequest()
        {
            return HttpContext.WebSockets.IsWebSocketRequest;
        }

        private bool CanAcceptGzip()
        {
            if (_context.AllowResponseCompression == false)
                return false;

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
            return GetIntQueryString(StartParameter, defaultValue);
        }

        protected int GetPageSize(int defaultValue = 25)
        {
            return GetIntQueryString(PageSizeParameter, defaultValue);
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

        protected string GetStringQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (required)
                    throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

                return null;
            }

            return val[0];
        }

        protected StringValues GetStringValuesQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (required)
                    throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

                return default(StringValues);
            }

            return val;
        }

        protected bool GetBoolValueQueryString(string name, bool required = true)
        {
            var boolAsString = GetStringQueryString(name, required: false);

            if (boolAsString == null)
            {
                if (required)
                    throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified");

                return false;
            }

            bool result;
            if (bool.TryParse(boolAsString, out result) == false)
            {
                if (required)
                    throw new ArgumentException($"Could not parse query string '{name}' as bool");

                return false;
            }

            return result;
        }

        protected DateTime? GetDateTimeQueryString(string name)
        {
            var dataAsString = GetStringQueryString(name, required: false);
            if (dataAsString == null)
                return null;

            dataAsString = Uri.UnescapeDataString(dataAsString);

            DateTime result;
            if (DateTime.TryParseExact(dataAsString, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
                return result;

            throw new ArgumentException($"Could not parse query string '{name}' as date");
        }
    }
}