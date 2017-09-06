using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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

            if (HttpContext.Request.Form.TryGetValue(itemName, out StringValues value) == false)
                return null;

            if (value.Count == 0)
                return null;

            return new MemoryStream(Encoding.UTF8.GetBytes(value[0]));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Stream RequestBodyStream()
        {
            return GetDecompressedStream(HttpContext.Request.Body, HttpContext.Request.Headers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Stream GetBodyStream(MultipartSection section)
        {
            return GetDecompressedStream(section.Body, section.Headers);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Stream GetDecompressedStream(Stream stream, IDictionary<string, StringValues> headers)
        {
            if (HeadersAllowGzip(headers, "Content-Encoding") == false)
                return stream;
            return GetGzipStream(stream, CompressionMode.Decompress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected GZipStream GetGzipStream(Stream stream, CompressionMode mode, CompressionLevel level = CompressionLevel.Fastest)
        {
            GZipStream gZipStream =
                mode == CompressionMode.Compress ?
                    new GZipStream(stream, level, true) :
                    new GZipStream(stream, mode, true);
            return gZipStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool AcceptsGzipResponse()
        {
            return _context.AllowResponseCompression && HeadersAllowGzip(HttpContext.Request.Headers, "Accept-Encoding");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HeadersAllowGzip(IDictionary<string, StringValues> headers, string encodingsHeader)
        {
            if (headers.TryGetValue(encodingsHeader, out StringValues acceptedContentEncodings) == false)
                return false;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var encoding in acceptedContentEncodings)
            {
                if (encoding.Contains("gzip") || encoding.Contains("deflate"))
                    return true;
            }

            return false;
        }

        protected Stream ResponseBodyStream() => HttpContext.Response.Body;

        /// <summary>
        /// Using this method requires disposing or closing of the returned
        /// compressed stream. Otherwise, the response will not be sent to
        /// the user
        /// </summary>
        /// <returns>A GZip-compressing stream</returns>
        protected GZipStream CompressedResponseBodyStream()
        {
            var responseBodyStream = HttpContext.Response.Body;

            if (AcceptsGzipResponse() == false)
                // We disable compression when the client does not accept it,
                // but the user of this function is expected to dispose the 
                // stream anyways
                return GetGzipStream(
                    responseBodyStream,
                    CompressionMode.Compress,
                    CompressionLevel.NoCompression);

            HttpContext.Response.Headers["Content-Encoding"] = "gzip";
            return GetGzipStream(responseBodyStream, CompressionMode.Compress);
        }

        protected bool IsWebsocketRequest()
        {
            return HttpContext.WebSockets.IsWebSocketRequest;
        }

        protected string GetStringFromHeaders(string name)
        {
            var headers = HttpContext.Request.Headers[name];
            if (headers.Count == 0)
                return null;

            if (headers[0].Length < 2)
                return headers[0];

            string raw = headers[0][0] == '\"'
                ? headers[0].Substring(1, headers[0].Length - 2)
                : headers[0];

            return raw;
        }

        //TODO: remove this
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

        protected void ThrowInvalidInteger(string name, string etag, string type = "int")
        {
            throw new ArgumentException($"Could not parse header '{name}' header as {type}, value was: {etag}");
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

            if (int.TryParse(intAsString, out int result) == false)
                ThrowInvalidInteger(name, intAsString);

            return result;
        }

        protected long GetLongQueryString(string name)
        {
            return GetLongQueryString(name, true).Value;
        }

        protected long? GetLongQueryString(string name, bool required)
        {
            var longAsString = GetStringQueryString(name, required);
            if (longAsString == null)
                return null;

            if (long.TryParse(longAsString, out long result) == false)
                ThrowInvalidInteger(name, longAsString, "long");

            return result;
        }

        protected float? GetFloatValueQueryString(string name, bool required = true)
        {
            var floatAsString = GetStringQueryString(name, required);
            if (floatAsString == null)
                return null;

            if (float.TryParse(floatAsString, out float result) == false)
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

        public static void ThrowRequiredPropertyNameInRequest(string name)
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

            if (bool.TryParse(boolAsString, out bool result) == false)
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

            if (DateTime.TryParseExact(dataAsString, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime result))
                return result;

            ThrowInvalidDateTime(name, dataAsString);
            return null; //unreachable
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

            if (TimeSpan.TryParse(timeSpanAsString, out TimeSpan result))
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
            if (values.Count == 0 || string.IsNullOrWhiteSpace(values[0]))
                InvalidEmptyValue(name);
            if (values.Count > 1)
                InvalidCountOfValues(name);
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

        protected bool IsClusterAdmin()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.NotYetValid:
                case RavenServer.AuthenticationStatus.Operator:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    Server.Router.UnlikelyFailAuthorization(HttpContext, null, feature);
                    return false;
                
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        protected bool IsOperator()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    Server.Router.UnlikelyFailAuthorization(HttpContext, null, feature);
                    return false;
                case RavenServer.AuthenticationStatus.Operator:
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }
        
        protected bool TryGetAllowedDbs(string dbName, out Dictionary<string, DatabaseAccess> dbs, bool requireAdmin)
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            dbs = null;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    Server.Router.UnlikelyFailAuthorization(HttpContext, dbName, null);
                    return false;
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                case RavenServer.AuthenticationStatus.Operator:
                    return true;
                case RavenServer.AuthenticationStatus.Allowed:
                    if (dbName != null && feature.CanAccess(dbName, requireAdmin) == false)
                    {
                        Server.Router.UnlikelyFailAuthorization(HttpContext, dbName, null);
                        return false;
                    }

                    dbs = feature.AuthorizedDatabases;
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        private static void ThrowInvalidAuthStatus(RavenServer.AuthenticationStatus? status)
        {
            throw new ArgumentOutOfRangeException("Unknown authentication status: " + status);
        }

        protected void SetupCORSHeaders()
        {
            // TODO: handle this properly when using https
            // https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS
            HttpContext.Response.Headers.Add("Access-Control-Allow-Origin", HttpContext.Request.Headers["Origin"]);
            HttpContext.Response.Headers.Add("Access-Control-Allow-Methods", "PUT, POST, GET, OPTIONS, DELETE");
            HttpContext.Response.Headers.Add("Access-Control-Allow-Headers", HttpContext.Request.Headers["Access-Control-Request-Headers"]);
            HttpContext.Response.Headers.Add("Access-Control-Max-Age", "86400");
        }

        protected void SetLicenseLimitResponse(LicenseLimit licenseLimit)
        {
            if (licenseLimit == null)
                throw new ArgumentNullException(nameof(licenseLimit));

            HttpContext.Response.StatusCode = (int)HttpStatusCode.PaymentRequired;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var blittable = EntityToBlittable.ConvertEntityToBlittable(licenseLimit, DocumentConventions.Default, context);
                context.Write(writer, blittable);
                writer.Flush();
            }
        }
    }
}
