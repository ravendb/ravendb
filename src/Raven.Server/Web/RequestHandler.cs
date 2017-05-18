using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Web
{
    public abstract class RequestHandler
    {
        public const string StartParameter = "start";

        public const string PageSizeParameter = "pageSize";

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

        protected Stream TryGetRequestFormStream(string itemName)
        {
            if (HttpContext.Request.HasFormContentType == false)
                return null;

            StringValues value;
            if (HttpContext.Request.Form.TryGetValue(itemName, out value) == false)
                return null;

            if (value.Count == 0)
                return null;

            return new MemoryStream(Encoding.UTF8.GetBytes(value[0]));
        }

        protected Stream RequestBodyStream()
        {
            var requestBodyStream = HttpContext.Request.Body;

            if (IsGzipRequest() == false)
                return requestBodyStream;

            var gZipStream = new GZipStream(requestBodyStream, CompressionMode.Decompress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        protected Stream GetBodyStream(MultipartSection section)
        {
            var bodyStream = section.Body;

            if (IsGzipRequest(section.Headers) == false)
                return bodyStream;

            var gZipStream = new GZipStream(bodyStream, CompressionMode.Decompress);
            HttpContext.Response.RegisterForDispose(gZipStream);
            return gZipStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsGzipRequest(Dictionary<string, StringValues> headers)
        {
            if (headers.TryGetValue("Content-Encoding", out StringValues values) == false)
                return false;
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var val in values)
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
            var headers = HttpContext.Request.Headers[name];
            if (headers.Count == 0)
                return null;

            string raw = headers[0][0] == '\"'
                ? headers[0].Substring(1, headers[0].Length - 2)
                : headers[0];

            var success = long.TryParse(raw, out var result);

            if (success)
                return result;

            return null;
        }

        protected void ThrowInvalidInteger(string name, string etag)
        {
            throw new ArgumentException("Could not parse header '" + name + "' header as int, value was: " + etag);
        }

        protected int GetStart(int defaultStart = 0)
        {
            return GetIntValueQueryString(StartParameter, required: false) ?? defaultStart;
        }

        protected int GetPageSize()
        {
            var pageSize = GetIntValueQueryString(PageSizeParameter, required: false);
            if (pageSize.HasValue == false)
                return int.MaxValue;

            return pageSize.Value;
        }

        protected int? GetIntValueQueryString(string name, bool required = true)
        {
            var intAsString = GetStringQueryString(name, required);
            if (intAsString == null)
                return null;

            int result;
            if (int.TryParse(intAsString, out result) == false)
                ThrowInvalidInteger(name, intAsString);

            return result;
        }

        protected long? GetLongQueryString(string name, bool required = true)
        {
            var longAsString = GetStringQueryString(name, required);
            if (longAsString == null)
                return null;

            long result;
            if (long.TryParse(longAsString, out result) == false)
                ThrowInvalidInteger(name, longAsString);

            return result;
        }

        protected float? GetFloatValueQueryString(string name, bool required = true)
        {
            var floatAsString = GetStringQueryString(name, required);
            if (floatAsString == null)
                return null;

            float result;
            if (float.TryParse(floatAsString, out result) == false)
                ThrowInvalidFloat(name, result);

            return result;
        }

        private static void ThrowInvalidFloat(string name, float result)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as float, value was: {result}");
        }

        protected string GetStringQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0 || string.IsNullOrWhiteSpace(val[0]))
            {
                if (required)
                    ThrowRequiredMember(name);

                return null;
            }

            return val[0];
        }

        private static void ThrowRequiredMember(string name)
        {
            throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified.");
        }

        public static void ThrowRequiredPropertyNameInRequset(string name)
        {
            throw new ArgumentException($"Request should have a property name '{name}' which is mandatory.");
        }

        protected StringValues GetStringValuesQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (required)
                    ThrowRequiredMember(name);

                return default(StringValues);
            }

            return val;
        }

        protected bool? GetBoolValueQueryString(string name, bool required = true)
        {
            var boolAsString = GetStringQueryString(name, required);
            if (boolAsString == null)
                return null;

            bool result;
            if (bool.TryParse(boolAsString, out result) == false)
                ThrowInvalidBoolean(name, boolAsString);

            return result;
        }

        private static void ThrowInvalidBoolean(string name, string val)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as bool, val {val}");
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

            ThrowInvalidDateTime(name, dataAsString);
            return null; //unreacahble
        }

        private static void ThrowInvalidDateTime(string name, string dataAsString)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as date, val {dataAsString}");
        }

        protected TimeSpan? GetTimeSpanQueryString(string name, bool required = true)
        {
            var timeSpanAsString = GetStringQueryString(name, required);
            if (timeSpanAsString == null)
                return null;

            timeSpanAsString = Uri.UnescapeDataString(timeSpanAsString);

            TimeSpan result;
            if (TimeSpan.TryParse(timeSpanAsString, out result))
                return result;

            ThrowInvalidTimeSpan(name, timeSpanAsString);
            return null;// unreachable
        }

        private static void ThrowInvalidTimeSpan(string name, string timeSpanAsString)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as timespan val {timeSpanAsString}");
        }

        protected string GetQueryStringValueAndAssertIfSingleAndNotEmpty(string name)
        {
            var values = HttpContext.Request.Query[name];
            if (values.Count != 1)
                InvalidCountOfValues(name);
            if (string.IsNullOrWhiteSpace(values[0]))
                InvalidEmptyValue(name);

            return values[0];
        }

        //TODO - Temporary for old client test. need to be deleted .
        protected string GetQueryStringValue(string name)
        {
            var values = HttpContext.Request.Query[name];
            if (values.Count != 1)
                return null;
            return values[0];
        }

        private static void InvalidEmptyValue(string name)
        {
            throw new ArgumentException($"Query string value '{name}' must have a non empty value");
        }

        private static void InvalidCountOfValues(string name)
        {
            throw new ArgumentException($"Query string value '{name}' must appear exactly once");
        }

        protected DisposableAction TrackRequestTime()
        {
            HttpContext.Response.Headers.Add(Constants.Headers.RequestTime, "1");
            return null; // TODO [ppekrol] we cannot write Headers after response have started without buffering
            //var sw = Stopwatch.StartNew();
            //return new DisposableAction(() => HttpContext.Response.Headers.Add(Constants.Headers.RequestTime, sw.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture)));
        }

        protected Task NoContent()
        {
            NoContentStatus();

            return Task.CompletedTask;
        }

        protected void NoContentStatus()
        {
            HttpContext.Response.Headers.Remove("Content-Type");
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
        }
    }
}