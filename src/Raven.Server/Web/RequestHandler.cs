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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Org.BouncyCastle.Asn1.Cms;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
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

        public X509Certificate2 GetCurrentCertificate()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            return feature?.Certificate;
        }

        public virtual void Init(RequestHandlerContext context)
        {
            _context = context;
        }

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

        protected async Task WaitForExecutionOnSpecificNode(TransactionOperationContext context, ClusterTopology clusterTopology, string node, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader

            using (var requester = ClusterRequestExecutor.CreateForSingleNode(clusterTopology.GetUrlFromTag(node), ServerStore.Server.Certificate.Certificate))
            {
                await requester.ExecuteAsync(new WaitForRaftIndexCommand(index), context);
            }
        }

        protected async Task WaitForExecutionOnRelevantNodes(JsonOperationContext context, string database, ClusterTopology clusterTopology, List<string> members, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader
            if (members.Count == 0)
                throw new InvalidOperationException("Cannot wait for execution when there are no nodes to execute ON.");

            var executors = new List<ClusterRequestExecutor>();

            try
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
                {
                    cts.CancelAfter(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan);

                    var waitingTasks = new List<Task<Exception>>();
                    List<Exception> exceptions = null;

                    foreach (var member in members)
                    {
                        var url = clusterTopology.GetUrlFromTag(member);
                        var executor = ClusterRequestExecutor.CreateForSingleNode(url, ServerStore.Server.Certificate.Certificate);
                        executors.Add(executor);
                        waitingTasks.Add(ExecuteTask(executor, member, cts.Token));
                    }

                    while (waitingTasks.Count > 0)
                    {
                        var task = await Task.WhenAny(waitingTasks);
                        waitingTasks.Remove(task);

                        if (task.Result == null)
                            continue;

                        var exception = task.Result.ExtractSingleInnerException();

                        if (exceptions == null)
                            exceptions = new List<Exception>();

                        exceptions.Add(exception);
                    }

                    if (exceptions != null)
                    {
                        var allTimeouts = true;
                        foreach (var exception in exceptions)
                        {
                            if (exception is OperationCanceledException)
                                continue;

                            allTimeouts = false;
                        }

                        var aggregateException = new AggregateException(exceptions);

                        if (allTimeouts)
                            throw new TimeoutException($"Waited too long for the raft command (number {index}) to be executed on any of the relevant nodes to this command.", aggregateException);

                        throw new InvalidDataException($"The database '{database}' was created but is not accessible, because all of the nodes on which this database was supposed to reside on, threw an exception.", aggregateException);
                    }
                }
            }
            finally
            {
                foreach (var executor in executors)
                {
                    executor.Dispose();
                }
            }

            async Task<Exception> ExecuteTask(RequestExecutor executor, string nodeTag, CancellationToken token)
            {
                try
                {
                    await executor.ExecuteAsync(new WaitForRaftIndexCommand(index), context, token: token);
                    return null;
                }
                catch (RavenException re) when (re.InnerException is HttpRequestException)
                {
                    // we want to throw for self-checks
                    if (nodeTag == ServerStore.NodeTag)
                        return re;

                    // ignore - we are ok when connection with a node cannot be established (test: AddDatabaseOnDisconnectedNode)
                    return null;
                }
                catch (Exception e)
                {
                    return e;
                }
            }
        }

        private Stream _responseStream;

        protected Stream ResponseBodyStream()
        {
            if (_responseStream != null)
                return _responseStream;

            _responseStream = new StreamWithTimeout(HttpContext.Response.Body);

            _context.HttpContext.Response.RegisterForDispose(_responseStream);

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
                var fromStudio = HttpContext.Request.Headers.ContainsKey(Constants.Headers.StudioVersion);
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

            if (DateTime.TryParseExact(dataAsString, DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime result))
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

        protected bool IsOperator()
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

                    RequestRouter.UnlikelyFailAuthorization(HttpContext, null, feature,
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
                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                case RavenServer.AuthenticationStatus.Expired:
                case RavenServer.AuthenticationStatus.NotYetValid:
                    if (Server.Configuration.Security.AuthenticationEnabled == false)
                        return true;

                    RequestRouter.UnlikelyFailAuthorization(HttpContext, dbName, null, requireAdmin ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser);
                    return false;
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                case RavenServer.AuthenticationStatus.Operator:
                    return true;
                case RavenServer.AuthenticationStatus.Allowed:
                    if (dbName != null && feature.CanAccess(dbName, requireAdmin) == false)
                    {
                        RequestRouter.UnlikelyFailAuthorization(HttpContext, dbName, feature, requireAdmin ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser);
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

        public static void SetupCORSHeaders(HttpContext httpContext, ServerStore serverStore, CorsMode corsMode)
        {
            httpContext.Response.Headers.Add("Vary", "Origin");

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
                    if (IsOriginAllowed(requestedOrigin, serverStore))
                        allowedOrigin = requestedOrigin;
                    break;
            }

            httpContext.Response.Headers.Add("Access-Control-Allow-Origin", allowedOrigin);
            httpContext.Response.Headers.Add("Access-Control-Allow-Methods", "PUT, POST, GET, OPTIONS, DELETE");
            httpContext.Response.Headers.Add("Access-Control-Allow-Headers", httpContext.Request.Headers["Access-Control-Request-Headers"]);
            httpContext.Response.Headers.Add("Access-Control-Max-Age", "86400");
        }

        private static bool IsOriginAllowed(string origin, ServerStore serverStore)
        {
            if (serverStore.Server.Certificate.Certificate == null)
            {
                // running in unsafe mode - since server can be access via multiple urls/aliases accept them 
                return true;
            }

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
            HttpContext.Response.Headers.Add("Location", leaderLocation);
        }

        protected virtual OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(ServerStore.ServerShutdown, HttpContext.RequestAborted);
        }

        protected virtual OperationCancelToken CreateOperationToken(TimeSpan cancelAfter)
        {
            return new OperationCancelToken(cancelAfter, ServerStore.ServerShutdown, HttpContext.RequestAborted);
        }
    }
}
