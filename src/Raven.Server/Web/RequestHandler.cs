using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
        public const string StartParameter = "start";

        public const string PageSizeParameter = "pageSize";

        internal static readonly HashSet<string> SafeCsrfMethods = new()
        {
            HttpMethod.Head.Method,
            HttpMethod.Options.Method,
            HttpMethod.Trace.Method
        };

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

        public X509Certificate2 GetCurrentCertificate()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            return feature?.Certificate;
        }

        public virtual void Init(RequestHandlerContext context)
        {
            _context = context;
            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }

        public abstract Task CheckForChanges(RequestHandlerContext context);

        protected Stream TryGetRequestFromStream(string itemName)
        {
            if (HttpContext.Request.HasFormContentType == false)
                return null;

            if (HttpContext.Request.Form.TryGetValue(itemName, out Microsoft.Extensions.Primitives.StringValues value) == false)
                return null;

            if (value.Count == 0)
                return null;

            return new MemoryStream(Encoding.UTF8.GetBytes(value[0]));
        }

        private Stream _requestBodyStream;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Stream RequestBodyStream()
        {
            if (_requestBodyStream != null)
                return _requestBodyStream;
            _requestBodyStream = new StreamWithTimeout(GetDecompressedStream(HttpContext.Request.Body, HttpContext.Request.Headers));

            if (TrafficWatchManager.HasRegisteredClients)
            {
                HttpContext.Items["RequestStream"] = _requestBodyStream;
            }

            _context.HttpContext.Response.RegisterForDispose(_requestBodyStream);

            return _requestBodyStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Stream GetBodyStream(MultipartSection section)
        {
            Stream stream = new StreamWithTimeout(GetDecompressedStream(section.Body, section.Headers));
            _context.HttpContext.Response.RegisterForDispose(stream);
            return stream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Stream GetDecompressedStream(Stream stream, IDictionary<string, Microsoft.Extensions.Primitives.StringValues> headers)
        {
            if (HeadersAllowGzip(headers, Constants.Headers.ContentEncoding) == false)
                return stream;
            return GetGzipStream(stream, CompressionMode.Decompress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static GZipStream GetGzipStream(Stream stream, CompressionMode mode, CompressionLevel level = CompressionLevel.Fastest)
        {
            GZipStream gZipStream =
                mode == CompressionMode.Compress ?
                    new GZipStream(stream, level, true) :
                    new GZipStream(stream, mode, true);
            return gZipStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool ClientAcceptsGzipResponse()
        {

            return
                Server.Configuration.Http.UseResponseCompression &&
                (HttpContext.Request.IsHttps == false ||
                    (HttpContext.Request.IsHttps && Server.Configuration.Http.AllowResponseCompressionOverHttps)) &&
                HeadersAllowGzip(HttpContext.Request.Headers, "Accept-Encoding");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool HeadersAllowGzip(IDictionary<string, Microsoft.Extensions.Primitives.StringValues> headers, string encodingsHeader)
        {
            if (headers.TryGetValue(encodingsHeader, out Microsoft.Extensions.Primitives.StringValues acceptedContentEncodings) == false)
                return false;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var encoding in acceptedContentEncodings)
            {
                if (encoding.Contains("gzip"))
                    return true;
            }

            return false;
        }

        public static void ValidateNodeForAddingToDb(string databaseName, string node, DatabaseRecord databaseRecord, ClusterTopology clusterTopology, RavenServer server, string baseMessage = null)
        {
            baseMessage ??= "Can't execute the operation";

            var databaseIsBeenDeleted = databaseRecord.DeletionInProgress != null &&
                                        databaseRecord.DeletionInProgress.TryGetValue(node, out var deletionInProgress) &&
                                        deletionInProgress != DeletionInProgressStatus.No;
            if (databaseIsBeenDeleted)
                throw new InvalidOperationException($"{baseMessage}, because the database {databaseName} is currently being deleted from node {node} (which is in the new topology)");

            var url = clusterTopology.GetUrlFromTag(node);
            if (url == null)
                throw new InvalidOperationException($"{baseMessage}, because node {node} (which is in the new topology) is not part of the cluster");

            if (databaseRecord.Encrypted && url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false && server.AllowEncryptedDatabasesOverHttp == false)
                throw new InvalidOperationException($"{baseMessage}, because database {databaseName} is encrypted but node {node} (which is in the new topology) doesn't have an SSL certificate.");
        }

        private Stream _responseStream;

        protected Stream ResponseBodyStream()
        {
            if (_responseStream != null)
                return _responseStream;

            _responseStream = new StreamWithTimeout(HttpContext.Response.Body);

            _context.HttpContext.Response.RegisterForDispose(_responseStream);

            if (TrafficWatchManager.HasRegisteredClients)
            {
                HttpContext.Items["ResponseStream"] = _responseStream;
            }

            return _responseStream;
        }

        protected bool IsWebsocketRequest()
        {
            return HttpContext.WebSockets.IsWebSocketRequest;
        }

        protected string GetRaftRequestIdFromQuery()
        {
            var guid = GetStringQueryString("raft-request-id", required: false);

            if (guid == null)
            {
#if DEBUG
                var fromStudio = HttpContext.Request.IsFromStudio();
                if (fromStudio)
                    guid = RaftIdGenerator.NewId();
#else
                guid = RaftIdGenerator.NewId();
#endif
            }

            return guid;
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

        protected static void ThrowInvalidInteger(string name, string etag, string type = "int")
        {
            throw new ArgumentException($"Could not parse header '{name}' header as {type}, value was: {etag}");
        }

        protected int GetStart(int defaultStart = 0)
        {
            return GetIntValueQueryString(StartParameter, required: false) ?? defaultStart;
        }

        protected int GetPageSize(int defaultPageSize = int.MaxValue)
        {
            var pageSize = GetIntValueQueryString(PageSizeParameter, required: false);
            if (pageSize.HasValue == false)
                return defaultPageSize;

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

        protected char? GetCharQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0 || string.IsNullOrWhiteSpace(val[0]))
            {
                if (required)
                    ThrowRequiredMember(name);

                return null;
            }

            var value = val[0];
            if (value.Length > 1)
                ThrowSingleCharacterRequired(name, value);

            return value[0];
        }

        private static void ThrowSingleCharacterRequired(string name, string value)
        {
            throw new InvalidOperationException($"Query string {name} is expecting single character, but got '{value}'.");
        }

        private static void ThrowRequiredMember(string name)
        {
            throw new ArgumentException($"Query string {name} is mandatory, but wasn't specified.");
        }

        public static void ThrowRequiredPropertyNameInRequest(string name)
        {
            throw new ArgumentException($"Request should have a property name '{name}' which is mandatory.");
        }

        protected Microsoft.Extensions.Primitives.StringValues GetStringValuesQueryString(string name, bool required = true)
        {
            var val = HttpContext.Request.Query[name];
            if (val.Count == 0)
            {
                if (required)
                    ThrowRequiredMember(name);

                return default;
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

        protected DateTime? GetDateTimeQueryString(string name, bool required = true)
        {
            var dataAsString = GetStringQueryString(name, required);
            if (dataAsString == null)
                return null;

            dataAsString = Uri.UnescapeDataString(dataAsString);

            if (DateTime.TryParseExact(dataAsString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime result))
                return result;

            ThrowInvalidDateTime(name, dataAsString);
            return null; //unreachable
        }

        protected static void ThrowInvalidDateTime(string name, string dataAsString)
        {
            throw new ArgumentException($"Could not parse query string '{name}' as date, val '{dataAsString}'");
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

        protected Task NoContent(HttpStatusCode statusCode = HttpStatusCode.NoContent)
        {
            NoContentStatus(statusCode);

            return Task.CompletedTask;
        }

        protected void NoContentStatus(HttpStatusCode statusCode = HttpStatusCode.NoContent)
        {
            HttpContext.Response.Headers.Remove("Content-Type");
            HttpContext.Response.StatusCode = (int)statusCode;
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
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.NotYetValid:
                case RavenServer.AuthenticationStatus.Operator:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    return false;
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        protected async Task<bool> IsOperatorAsync()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    await RequestRouter.UnlikelyFailAuthorizationAsync(HttpContext, null, feature,
                        AuthorizationStatus.Operator);
                    return false;
                case RavenServer.AuthenticationStatus.Operator:
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                default:
                    ThrowInvalidAuthStatus(status);
                    return false;
            }
        }

        public class AllowedDbs
        {
            public bool HasAccess { get; set; }

            public Dictionary<string, DatabaseAccess> AuthorizedDatabases { get; set; }
        }

        protected async Task<bool> CanAccessDatabaseAsync(string dbName, bool requireAdmin, bool requireWrite)
        {
            var result = await GetAllowedDbsAsync(dbName, requireAdmin, requireWrite);

            return result.HasAccess;
        }

        protected async Task<AllowedDbs> GetAllowedDbsAsync(string dbName, bool requireAdmin, bool requireWrite)
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            var status = feature?.Status;
            switch (status)
            {
                case null:
                case RavenServer.AuthenticationStatus.None:
                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return new AllowedDbs { HasAccess = true };

                    await RequestRouter.UnlikelyFailAuthorizationAsync(HttpContext, dbName, null, requireAdmin ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser);
                    return new AllowedDbs { HasAccess = false };
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                case RavenServer.AuthenticationStatus.Operator:
                    return new AllowedDbs { HasAccess = true };
                case RavenServer.AuthenticationStatus.Allowed:
                    if (dbName != null && feature.CanAccess(dbName, requireAdmin, requireWrite) == false)
                    {
                        await RequestRouter.UnlikelyFailAuthorizationAsync(HttpContext, dbName, feature, requireAdmin ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser);
                        return new AllowedDbs { HasAccess = false };
                    }

                    return new AllowedDbs { HasAccess = true, AuthorizedDatabases = feature.AuthorizedDatabases };
                default:
                    ThrowInvalidAuthStatus(status);
                    return new AllowedDbs { HasAccess = false };
            }
        }

        private static void ThrowInvalidAuthStatus(RavenServer.AuthenticationStatus? status)
        {
            throw new ArgumentOutOfRangeException("Unknown authentication status: " + status);
        }

        public static bool CheckCSRF(HttpContext httpContext, ServerStore serverStore)
        {
            if (serverStore.Configuration.Security.EnableCsrfFilter == false)
                return true;
            
            var requestedOrigin = httpContext.Request.Headers[Constants.Headers.Origin];
            
            if (requestedOrigin.Count == 0 || requestedOrigin[0] == null)
                return true;
            
            // no origin at this point - it means it is safe request or non-browser

            var host = httpContext.Request.Host;
            if (string.IsNullOrEmpty(host.Host))
                return false;
            
            if (SafeCsrfMethods.Contains(httpContext.Request.Method))
                return true;
            
            var origin = requestedOrigin[0];
            var uriOrigin = new Uri(origin);
            var originHost = uriOrigin.Host;
            var originAuthority = uriOrigin.Authority;
            
            // for hostname matching we validate both hostname and port
            var hostMatches = host.ToString() == originAuthority;
            if (hostMatches)
                return true;
            
            // for requests with-in cluster we value both hostname and port
            var requestWithinCluster = IsOriginAllowed(origin, serverStore);
            if (requestWithinCluster)
                return true;
            
            // for trusted origins we match hostname only, port is ignored
            var trustedOrigins = serverStore.Configuration.Security.CsrfTrustedOrigins ?? Array.Empty<string>();
            if (trustedOrigins.Length > 0)
            {
                foreach (var o in trustedOrigins)
                {
                    if (originHost == o)
                        return true;
                }
            }

            // for additional origin headers we match hostname only, port is ignored
            var additionalHeaders = serverStore.Configuration.Security.CsrfAdditionalOriginHeaders ?? Array.Empty<string>();
            if (additionalHeaders.Length > 0)
            {
                foreach (string additionalHeader in additionalHeaders)
                {
                    if (httpContext.Request.Headers.TryGetValue(additionalHeader, out var headerValue) == false)
                        continue;

                    var stringHeader = headerValue.ToString();

                    if (stringHeader == originAuthority)
                        return true;
                }
            }

            return false;
        }
        
        public static void SetupCORSHeaders(HttpContext httpContext, ServerStore serverStore, CorsMode corsMode)
        {
            httpContext.Response.Headers["Vary"] = "Origin";

            var requestedOrigin = httpContext.Request.Headers["Origin"];

            if (requestedOrigin.Count == 0)
            {
                // no CORS headers needed
                return;
            }

            string allowedOrigin = null; // prevent access by default

            switch (corsMode)
            {
                case CorsMode.Public:
                    allowedOrigin = requestedOrigin;
                    break;
                case CorsMode.Cluster:
                    if (serverStore.Server.Certificate.Certificate == null || IsOriginAllowed(requestedOrigin, serverStore))
                        allowedOrigin = requestedOrigin;
                    break;
            }

            httpContext.Response.Headers["Access-Control-Allow-Origin"] = allowedOrigin;
            httpContext.Response.Headers["Access-Control-Allow-Methods"] = "PUT, POST, GET, OPTIONS, DELETE";
            httpContext.Response.Headers["Access-Control-Allow-Headers"] = httpContext.Request.Headers["Access-Control-Request-Headers"];
            httpContext.Response.Headers["Access-Control-Max-Age"] = "86400";
        }

        private static bool IsOriginAllowed(string origin, ServerStore serverStore)
        {
            var topology = serverStore.GetClusterTopology();

            // check explicitly each topology type to avoid allocations in topology.AllNodes
            foreach (var kvp in topology.Members)
            {
                if (kvp.Value.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            foreach (var kvp in topology.Watchers)
            {
                if (kvp.Value.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            foreach (var kvp in topology.Promotables)
            {
                if (kvp.Value.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }
        protected void RedirectToLeader()
        {
            if (ServerStore.LeaderTag == null)
                throw new NoLeaderException();

            if (ServerStore.Engine.CurrentState == RachisState.LeaderElect)
                throw new NoLeaderException("This node is elected to be the leader, but didn't took office yet.");

            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.GetClusterTopology(context);
            }
            var url = topology.GetUrlFromTag(ServerStore.LeaderTag);
            if (string.Equals(url, ServerStore.GetNodeHttpServerUrl(), StringComparison.OrdinalIgnoreCase))
            {
                throw new NoLeaderException($"This node is not the leader, but the current topology does mark it as the leader. Such confusion is usually an indication of a network or configuration problem.");
            }
            var leaderLocation = url + HttpContext.Request.Path + HttpContext.Request.QueryString;
            HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
            HttpContext.Response.Headers.Remove("Content-Type");
            HttpContext.Response.Headers["Location"] = leaderLocation;
        }

        protected virtual OperationCancelToken CreateHttpRequestBoundOperationToken()
        {
            return new OperationCancelToken(ServerStore.ServerShutdown, HttpContext.RequestAborted);
        }

        protected virtual OperationCancelToken CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, ServerStore.ServerShutdown, HttpContext.RequestAborted);
        }

        protected virtual OperationCancelToken CreateBackgroundOperationToken()
        {
            return new OperationCancelToken(ServerStore.ServerShutdown);
        }

        /// <summary>
        /// puts the given string in TrafficWatch property of HttpContext.Items
        /// puts the given type in TrafficWatchChangeType property of HttpContext.Items
        /// </summary>
        /// <param name="str"></param>
        /// <param name="type"></param>
        public void AddStringToHttpContext(string str, TrafficWatchChangeType type)
        {
            HttpContext.Items["TrafficWatch"] = (str, type);
        }
    }
}
