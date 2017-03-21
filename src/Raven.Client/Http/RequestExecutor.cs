using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http.OAuth;
using Raven.Client.Server.Commands;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Http
{
    public class RequestExecutor : IDisposable
    {

        // https://aspnetmonsters.com/2016/08/2016-08-27-httpclientwrong/

        private static readonly TimeSpan GlobalHttpClientTimeout = TimeSpan.FromHours(12);

        private static readonly Lazy<HttpClient> GlobalHttpClient = new Lazy<HttpClient>(() => CreateClient(GlobalHttpClientTimeout));

        private readonly string _apiKey;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecutor>("Client");

        public readonly JsonContextPool ContextPool;

        public class AggresiveCacheOptions
        {
            public TimeSpan? Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly AsyncLocal<AggresiveCacheOptions> AggressiveCaching = new AsyncLocal<AggresiveCacheOptions>();

        public readonly HttpCache Cache = new HttpCache();

        private Topology _topology;
        private readonly Timer _updateTopologyTimer;

        private Timer _updateCurrentTokenTimer;
        private readonly Timer _updateFailingNodesStatus;

        public string Url => _topology.LeaderNode.Url;

        private RequestExecutor(string url, string databaseName, string apiKey, bool requiresTopologyUpdates)
        {
            _apiKey = apiKey;
            _topology = new Topology
            {
                LeaderNode = new ServerNode
                {
                    Url = url,
                    Database = databaseName,
                },
                ReadBehavior = ReadBehavior.LeaderOnly,
                WriteBehavior = WriteBehavior.LeaderOnly,
                Etag = int.MinValue,
                SLA = new TopologySla
                {
                    RequestTimeThresholdInMilliseconds = 100
                }
            };

            ContextPool = new JsonContextPool();

            if (requiresTopologyUpdates == false)
                return;

            _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, 0, Timeout.Infinite);
            _updateFailingNodesStatus = new Timer(UpdateFailingNodesStatusCallback, null, TimeSpan.FromMinutes(1),
                TimeSpan.FromMinutes(1));

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var node = _topology.LeaderNode;
                var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

                var cachedTopology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, context);
                if (cachedTopology != null && cachedTopology.Etag > _topology.Etag)
                {
                    _topology = cachedTopology;
                }
            }
        }

        public string ApiKey => _apiKey;

        public static RequestExecutor Create(string url, string databaseName, string apiKey)
        {
            return new RequestExecutor(url, databaseName, apiKey, requiresTopologyUpdates: true);
        }

        public static RequestExecutor CreateForSingleNode(string url, string databaseName, string apiKey)
        {
            return new RequestExecutor(url, databaseName, apiKey, requiresTopologyUpdates: false);
        }

        protected virtual void UpdateTopologyCallback(object _)
        {
            GC.KeepAlive(UpdateTopology());
        }

        public async Task<bool> UpdateTopology()
        {
            if (_disposed)
                return false;
            bool lookTaken = false;
            Monitor.TryEnter(this, 0, ref lookTaken);
            try
            {
                if (_disposed)
                    return false;
                JsonOperationContext context;
                var operationContext = ContextPool.AllocateOperationContext(out context);
                try
                {

                    lookTaken = false;
                    Monitor.Exit(this); // don't lock now, we aren't using any more shared resources that require protection

                    var node = _topology.LeaderNode;
                    var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

                    var command = new GetTopologyCommand();
                    try
                    {

                        await ExecuteAsync(new ChoosenNode { Node = node }, context, command);

                        if (UpdateTopologyField(command.Result))
                        {
                            TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _topology, context);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failed to update topology", ex);
                        return false;
                    }
                    finally
                    {
                        if (_disposed == false)
                            _updateTopologyTimer.Change(TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);
                    }
                }
                finally
                {
                    operationContext?.Dispose();
                }
            }
            finally
            {
                if (lookTaken)
                    Monitor.Exit(this);
            }
            return true;
        }

        private bool UpdateTopologyField(Topology topology)
        {
            if (topology == null)
                return false;

            Debug.Assert(topology.LeaderNode != null);

            var oldTopology = _topology;
            do
            {
                if (oldTopology.Etag >= topology.Etag)
                    return false;

                var changed = Interlocked.CompareExchange(ref _topology, topology, oldTopology);
                if (changed == oldTopology)
                    return true;
                oldTopology = changed;
            } while (true);
        }

        public void Execute<TResult>(RavenCommand<TResult> command, JsonOperationContext context)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context));
        }

        public async Task ExecuteAsync<TResult>(RavenCommand<TResult> command, JsonOperationContext context, CancellationToken token = default(CancellationToken))
        {
            var choosenNode = ChooseNodeForRequest(command);

            await ExecuteAsync(choosenNode, context, command, token).ConfigureAwait(false);
        }

        public async Task ExecuteAsync<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command, CancellationToken token = default(CancellationToken))
        {
            string url;
            var request = CreateRequest(choosenNode.Node, command, out url);

            long cachedEtag;
            BlittableJsonReaderObject cachedValue;
            HttpCache.ReleaseCacheItem cachedItem;
            using (cachedItem = GetFromCache(context, command, request, url, out cachedEtag, out cachedValue))
            {
                if (cachedEtag != 0)
                {
                    var aggresiveCacheOptions = AggressiveCaching.Value;
                    if (aggresiveCacheOptions != null && cachedItem.Age < aggresiveCacheOptions.Duration)
                    {
                        command.SetResponse(cachedValue, fromCache: true);
                        return;
                    }

                    request.Headers.TryAddWithoutValidation("If-None-Match", $"\"{cachedEtag}\"");
                }

                var sp = Stopwatch.StartNew();
                HttpResponseMessage response;
                try
                {
                    var client = GetHttpClientForCommand(command);
                    if (command.Timeout.HasValue)
                    {
                        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, CancellationToken.None))
                        {
                            cts.CancelAfter(command.Timeout.Value);
                            response = await client.SendAsync(request, cts.Token).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        response = await GetHttpClientForCommand(command).SendAsync(request, token).ConfigureAwait(false);
                    }

                    sp.Stop();
                }
                catch (HttpRequestException e) // server down, network down
                {
                    sp.Stop();
                    if (await HandleServerDown(choosenNode, context, command, request, e) == false)
                        throw;
                    return;
                }
                finally
                {
                    var requestTimeInMilliseconds = sp.ElapsedMilliseconds;
                    choosenNode.Node.UpdateRequestTime(requestTimeInMilliseconds);
                    if (choosenNode.SkippedNodes != null)
                    {
                        foreach (var skippedNode in choosenNode.SkippedNodes)
                        {
                            skippedNode.DecreaseRate(requestTimeInMilliseconds);
                        }
                    }
                }

                command.StatusCode = response.StatusCode;

                if (response.StatusCode == HttpStatusCode.NotModified)
                {
                    cachedItem.NotModified();
                    command.SetResponse(cachedValue, fromCache: true);
                    return;
                }
                if (response.IsSuccessStatusCode == false)
                {
                    if (await HandleUnsuccessfulResponse(choosenNode, context, command, request, response, url).ConfigureAwait(false))
                        return;
                }

                await command.ProcessResponse(context, Cache, response, url).ConfigureAwait(false);
            }
        }

        private HttpCache.ReleaseCacheItem GetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, string url, out long cachedEtag, out BlittableJsonReaderObject cachedValue)
        {
            if (command.IsReadRequest && command.ResponseType != RavenCommandResponseType.Stream)
            {
                if (request.Method != HttpMethod.Get)
                    url = request.Method + "-" + url;
                return Cache.Get(context, url, out cachedEtag, out cachedValue);
            }

            cachedEtag = 0;
            cachedValue = null;
            return new HttpCache.ReleaseCacheItem();
        }

        public static readonly string ClientVersion = typeof(RequestExecutor).GetTypeInfo().Assembly.GetName().Version.ToString();

        private static HttpRequestMessage CreateRequest<TResult>(ServerNode node, RavenCommand<TResult> command, out string url)
        {
            var request = command.CreateRequest(node, out url);

            request.RequestUri = new Uri(url);

            if (node.CurrentToken != null)
                request.Headers.Add("Raven-Authorization", node.CurrentToken);

            if (!request.Headers.Contains("Raven-Client-Version"))
                request.Headers.Add("Raven-Client-Version", ClientVersion);
            return request;
        }

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, string url)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse((BlittableJsonReaderObject)null, fromCache: false);
                    else if (command.ResponseType == RavenCommandResponseType.Array)
                        command.SetResponse((BlittableJsonReaderArray)null, fromCache: false);
                    else
                        command.SetResponseUncached(response, null);
                    return true;
                case HttpStatusCode.Unauthorized:
                case HttpStatusCode.PreconditionFailed:
                    if (string.IsNullOrEmpty(_apiKey))
                        throw AuthorizationException.EmptyApiKey(url);
                    if (++command.AuthenticationRetries > 1)
                        throw AuthorizationException.Unauthorized(url);

                    string oauthSource = null;
                    IEnumerable<string> values;
                    if (response.Headers.TryGetValues("OAuth-Source", out values))
                        oauthSource = values.FirstOrDefault();

#if DEBUG && FIDDLER
// Make sure to avoid a cross DNS security issue, when running with Fiddler
                if (string.IsNullOrEmpty(oauthSource) == false)
                    oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

                    await HandleUnauthorized(oauthSource, choosenNode.Node, context).ConfigureAwait(false);
                    await ExecuteAsync(choosenNode, context, command).ConfigureAwait(false);
                    return true;
                case HttpStatusCode.Forbidden:
                    throw AuthorizationException.Forbidden(url);
                case HttpStatusCode.GatewayTimeout:
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    await HandleServerDown(choosenNode, context, command, request, null).ConfigureAwait(false);
                    break;
                case HttpStatusCode.Conflict:
                    await HandleConflict(context, response).ConfigureAwait(false);
                    break;
                default:
                    await ExceptionDispatcher.Throw(context, response).ConfigureAwait(false);
                    break;
            }
            return false;
        }

        private static Task HandleConflict(JsonOperationContext context, HttpResponseMessage response)
        {
            // TODO: Conflict resolution
            // current implementation is temporary 

            return ExceptionDispatcher.Throw(context, response);
        }

        public static async Task<MemoryStream> ReadAsStreamUncompressedAsync(HttpResponseMessage response)
        {
            using (var serverStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var stream = serverStream;
                var encoding = response.Content.Headers.ContentEncoding.FirstOrDefault();
                if (encoding != null && encoding.Contains("gzip"))
                    stream = new GZipStream(stream, CompressionMode.Decompress);
                else if (encoding != null && encoding.Contains("deflate"))
                    stream = new DeflateStream(stream, CompressionMode.Decompress);
                var ms = new MemoryStream();

                var buffer = new byte[4096];
                int read;
                while ((read = stream.Read(buffer, 0, buffer.Length)) != 0)
                {
                    ms.Write(buffer, 0, read);
                }
                stream.Dispose();
                ms.Position = 0;
                return ms;
            }
        }

        private async Task<bool> HandleServerDown<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpRequestException e)
        {
            if (command.AvoidFailover)
                return false;

            if (command.FailedNodes == null)
                command.FailedNodes = new HashSet<ServerNode>();

            choosenNode.Node.IsFailed = true;
            command.FailedNodes.Add(choosenNode.Node);

            var failoverNode = ChooseNodeForRequest(command, request, e);
            await ExecuteAsync(failoverNode, context, command);

            return true;
        }

        public string UrlFor(string documentKey)
        {
            var node = _topology.LeaderNode;
            return $"{node.Url}/databases/{node.Database}/docs?id={documentKey}";
        }

        public class ChoosenNode
        {
            public ServerNode Node;
            public List<ServerNode> SkippedNodes;
        }

        private ChoosenNode ChooseNodeForRequest<T>(RavenCommand<T> command, HttpRequestMessage request = null, HttpRequestException exception = null)
        {
            var topology = _topology;

            var leaderNode = topology.LeaderNode;
            Debug.Assert(leaderNode != null);

            if (command.IsReadRequest)
            {
                if (topology.ReadBehavior == ReadBehavior.LeaderOnly)
                {
                    if (command.IsFailedWithNode(leaderNode) == false)
                        return new ChoosenNode { Node = leaderNode };
                    ThrowNoFailoverPossible(command, request, exception);
                }

                if (topology.ReadBehavior == ReadBehavior.RoundRobin)
                {
                    if (leaderNode.IsFailed == false && command.IsFailedWithNode(leaderNode) == false)
                        return new ChoosenNode { Node = leaderNode };

                    // TODO: Should we choose nodes here by rate value as for SLA?
                    var choosenNode = new ChoosenNode { SkippedNodes = new List<ServerNode>() };
                    foreach (var node in topology.Nodes)
                    {
                        if (node.IsFailed == false && command.IsFailedWithNode(node) == false)
                        {
                            choosenNode.Node = node;
                            return choosenNode;
                        }

                        choosenNode.SkippedNodes.Add(node);
                    }

                    throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", exception);
                }

                if (topology.ReadBehavior == ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached)
                {
                    if (leaderNode.IsFailed == false && command.IsFailedWithNode(leaderNode) == false && leaderNode.IsRateSurpassed(topology.SLA.RequestTimeThresholdInMilliseconds))
                        return new ChoosenNode { Node = leaderNode };

                    var nodesWithLeader = topology.Nodes
                        .Union(new[] { leaderNode })
                        .OrderBy(node => node.Rate())
                        .ToList();
                    var fastestNode = nodesWithLeader.FirstOrDefault(node => node.IsFailed == false && command.IsFailedWithNode(node) == false);
                    nodesWithLeader.Remove(fastestNode);

                    var choosenNode = new ChoosenNode
                    {
                        Node = fastestNode,
                        SkippedNodes = nodesWithLeader
                    };

                    if (choosenNode.Node != null)
                        return choosenNode;

                    throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", exception);
                }

                throw new InvalidOperationException($"Invalid ReadBehaviour value: {topology.ReadBehavior}");
            }

            if (topology.WriteBehavior == WriteBehavior.LeaderOnly)
            {
                if (command.IsFailedWithNode(leaderNode) == false)
                    return new ChoosenNode { Node = leaderNode };
                ThrowNoFailoverPossible(command, request, exception);
            }

            if (topology.WriteBehavior == WriteBehavior.LeaderWithFailover)
            {
                if (leaderNode.IsFailed == false && command.IsFailedWithNode(leaderNode) == false)
                    return new ChoosenNode { Node = leaderNode };

                // TODO: Should we choose nodes here by rate value as for SLA?
                foreach (var node in topology.Nodes)
                {
                    if (node.IsFailed == false && command.IsFailedWithNode(node) == false)
                        return new ChoosenNode { Node = node };
                }

                throw new HttpRequestException("Tried all nodes in the cluster but failed getting a response", exception);
            }

            throw new InvalidOperationException($"Invalid WriteBehavior value: {topology.WriteBehavior}");
        }

        private static void ThrowNoFailoverPossible<T>(RavenCommand<T> command, HttpRequestMessage request, HttpRequestException exception)
        {
            throw new HttpRequestException(
                $"Leader node has failed to make the request {command.GetType().Name} - {request?.Method} {request?.RequestUri}. The current ReadBehavior is set to Leader Only failover to a different node is not authorized.",
                exception);
        }

        public async Task<string> GetAuthenticationToken(JsonOperationContext context, ServerNode node)
        {
            return await _authenticator.GetAuthenticationTokenAsync(_apiKey, node.Url, context).ConfigureAwait(false);
        }

        private async Task HandleUnauthorized(string oauthSource, ServerNode node, JsonOperationContext context, bool shouldThrow = true)
        {
            try
            {
                if (string.IsNullOrEmpty(oauthSource))
                    oauthSource = node.Url + "/OAuth/API-Key";

                var currentToken = await _authenticator.AuthenticateAsync(oauthSource, _apiKey, context).ConfigureAwait(false);
                node.CurrentToken = currentToken;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to authorize using api key", e);

                if (shouldThrow)
                    throw;
            }

            if (_updateCurrentTokenTimer == null)
            {
                _updateCurrentTokenTimer = new Timer(UpdateCurrentTokenCallback, null, TimeSpan.FromMinutes(20), TimeSpan.FromMinutes(20));
            }
        }

        private void UpdateCurrentTokenCallback(object _)
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var topology = _topology;

                var leaderNode = topology.LeaderNode;
                if (leaderNode != null)
                {
#pragma warning disable 4014
                    HandleUnauthorized(null, leaderNode, context, shouldThrow: false);
#pragma warning restore 4014
                }

                foreach (var node in topology.Nodes)
                {
#pragma warning disable 4014
                    HandleUnauthorized(null, node, context, shouldThrow: false);
#pragma warning restore 4014
                }
            }
        }

        private readonly object _updateFailingNodeStatusLock = new object();
        private bool _disposed;

        protected virtual void UpdateFailingNodesStatusCallback(object _)
        {
            if (Monitor.TryEnter(_updateFailingNodeStatusLock) == false)
                return;

            try
            {
                var topology = _topology;
                var tasks = new List<Task>();

                var leaderNode = topology.LeaderNode;
                if (leaderNode?.IsFailed ?? false)
                {
                    tasks.Add(TestIfNodeAlive(leaderNode));
                }

                var serverNodes = topology.Nodes;
                if (serverNodes != null)
                {
                    for (var i = 1; i < serverNodes.Count; i++)
                    {
                        var node = serverNodes[i];
                        if (node?.IsFailed ?? false)
                        {
                            tasks.Add(TestIfNodeAlive(node));
                        }
                    }
                }

                if (tasks.Count == 0)
                    return;

                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Failed to check if failing server are down", e);
                }
            }
            finally
            {
                Monitor.Exit(_updateFailingNodeStatusLock);
            }
        }

        private static async Task TestIfNodeAlive(ServerNode node)
        {
            var command = new GetTopologyCommand();
            string url;
            var request = CreateRequest(node, command, out url);

            var sp = Stopwatch.StartNew();
            try
            {
                var response = await GlobalHttpClient.Value.SendAsync(request).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    node.IsFailed = false;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Tested if node alive but it's down: {url}", e);
            }
            finally
            {
                sp.Stop();
                node.UpdateRequestTime(sp.ElapsedMilliseconds);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            lock (this)
            {
                if (_disposed)
                    return;
                _disposed = true;
                Cache.Dispose();
                ContextPool.Dispose();
                _updateCurrentTokenTimer?.Dispose();
                _updateFailingNodesStatus?.Dispose();
                _updateTopologyTimer?.Dispose();
                // shared instance, cannot dispose!
                //_httpClient.Dispose();
            }
        }

        private static HttpClient CreateClient(TimeSpan timeout)
        {
            var httpMessageHandler = new HttpClientHandler();
            return new HttpClient(httpMessageHandler)
            {
                Timeout = timeout
            };
        }

        private static HttpClient GetHttpClientForCommand<T>(RavenCommand<T> command)
        {
            var timeout = command.Timeout;
            if (timeout.HasValue && timeout > GlobalHttpClientTimeout)
                throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{timeout}'.");

            return GlobalHttpClient.Value;
        }
    }
}