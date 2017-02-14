using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
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
    public class RequestExecuter : IDisposable
    {

        // https://aspnetmonsters.com/2016/08/2016-08-27-httpclientwrong/

        private static readonly Lazy<HttpClient> GlobalHttpClient = new Lazy<HttpClient>(() =>
        {
            var httpMessageHandler = new HttpClientHandler();
            return new HttpClient(httpMessageHandler);
        });

        private readonly string _apiKey;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecuter>("Client");

        public readonly JsonContextPool ContextPool;

        public class AggresiveCacheOptions
        {
            public TimeSpan? Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly AsyncLocal<AggresiveCacheOptions> AggressiveCaching = new AsyncLocal<AggresiveCacheOptions>();

        public readonly HttpCache Cache = new HttpCache();

        private readonly HttpClient _httpClient = GlobalHttpClient.Value;

        private Topology _topology;
        private readonly Timer _updateTopologyTimer;
        private bool _firstTimeTryLoadFromTopologyCache = true;

        private Timer _updateCurrentTokenTimer;
        private readonly Timer _updateFailingNodesStatus;

        public RequestExecuter(string url, string databaseName, string apiKey)
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
            };

            ContextPool = new JsonContextPool();

            _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, 0, Timeout.Infinite);
            _updateFailingNodesStatus = new Timer(UpdateFailingNodesStatusCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        public static RequestExecuter ShortTermSingleUse(string url, string databaseName, string apiKey)
        {
            return new ShortTermSingleUseRequestExecuter(url, databaseName, apiKey);
        }

        protected virtual void UpdateTopologyCallback(object _)
        {
            GC.KeepAlive(UpdateTopology());
        }

        private async Task UpdateTopology()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var node = _topology.LeaderNode;

                var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

                if (_firstTimeTryLoadFromTopologyCache)
                {
                    _firstTimeTryLoadFromTopologyCache = false;

                    var cachedTopology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, context);
                    if (cachedTopology != null && cachedTopology.Etag > 0)
                    {
                        _topology = cachedTopology;
                        // we have cached topology, but we need to verify it is up to date, we'll check in 
                        // 1 second, and let the rest of the system start
                        _updateTopologyTimer.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
                        return;
                    }
                }

                var command = new GetTopologyCommand();
                try
                {
                    await ExecuteAsync(new ChoosenNode { Node = node }, context, command);
                    if (_topology.Etag != command.Result.Etag)
                    {
                        _topology = command.Result;
                        TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _topology, context);
                    }
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Failed to update topology", ex);
                }
                finally
                {
                    _updateTopologyTimer.Change(TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);
                }
            }
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

                    request.Headers.IfNoneMatch.Add(new EntityTagHeaderValue("\"" + cachedEtag + "\""));
                }

                var sp = Stopwatch.StartNew();
                HttpResponseMessage response;
                try
                {
                    response = await _httpClient.SendAsync(request, token).ConfigureAwait(false);
                    sp.Stop();
                }
                catch (HttpRequestException e) // server down, network down
                {
                    sp.Stop();
                    await HandleServerDown(choosenNode, context, command, e);
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
                    if (await HandleUnsuccessfulResponse(choosenNode, context, command, response, url).ConfigureAwait(false))
                        return;
                }

                await command.ProcessResponse(context, Cache, response, url).ConfigureAwait(false);
            }
        }

        private HttpCache.ReleaseCacheItem GetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, string url, out long cachedEtag, out BlittableJsonReaderObject cachedValue)
        {
            if (command.IsReadRequest)
            {
                if (request.Method != HttpMethod.Get)
                    url = request.Method + "-" + url;
                return Cache.Get(context, url, out cachedEtag, out cachedValue);
            }

            cachedEtag = 0;
            cachedValue = null;
            return new HttpCache.ReleaseCacheItem();
        }

        public static readonly string ClientVersion = typeof(RequestExecuter).GetTypeInfo().Assembly.GetName().Version.ToString();

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

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command,
            HttpResponseMessage response, string url)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse((BlittableJsonReaderObject)null, fromCache: false);
                    else
                        command.SetResponse((BlittableJsonReaderArray)null, fromCache: false);
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
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    await HandleServerDown(choosenNode, context, command, null).ConfigureAwait(false);
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

        private async Task HandleServerDown<TResult>(ChoosenNode choosenNode, JsonOperationContext context, RavenCommand<TResult> command,
            HttpRequestException e)
        {
            if (command.FailedNodes == null)
                command.FailedNodes = new HashSet<ServerNode>();

            choosenNode.Node.IsFailed = true;
            command.FailedNodes.Add(choosenNode.Node);

            var failoverNode = ChooseNodeForRequest(command, e);
            await ExecuteAsync(failoverNode, context, command);
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

        private ChoosenNode ChooseNodeForRequest<T>(RavenCommand<T> command, HttpRequestException exception = null)
        {
            var topology = _topology;

            var leaderNode = topology.LeaderNode;

            if (command.IsReadRequest)
            {
                if (topology.ReadBehavior == ReadBehavior.LeaderOnly)
                {
                    if (command.IsFailedWithNode(leaderNode) == false)
                        return new ChoosenNode { Node = leaderNode };
                    throw new HttpRequestException("Leader not was failed to make this request. The current ReadBehavior is set to Leader to we won't failover to a differnt node.", exception);
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
                throw new HttpRequestException("Leader not was failed to make this request. The current WriteBehavior is set to Leader to we won't failover to a differnt node.", exception);
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

        private async Task TestIfNodeAlive(ServerNode node)
        {
            var command = new GetTopologyCommand();
            string url;
            var request = CreateRequest(node, command, out url);

            var sp = Stopwatch.StartNew();
            try
            {
                var response = await _httpClient.SendAsync(request).ConfigureAwait(false);
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
            Cache.Dispose();
            _authenticator.Dispose();
            ContextPool.Dispose();
            _updateCurrentTokenTimer?.Dispose();
            _updateFailingNodesStatus?.Dispose();
            // shared instance, cannot dispose!
            //_httpClient.Dispose();
        }


        private class ShortTermSingleUseRequestExecuter : RequestExecuter
        {
            public ShortTermSingleUseRequestExecuter(string url, string databaseName, string apiKey)
                : base(url, databaseName, apiKey)
            {
            }

            protected override void UpdateTopologyCallback(object _)
            {
            }

            protected override void UpdateFailingNodesStatusCallback(object _)
            {
            }
        }
    }
}