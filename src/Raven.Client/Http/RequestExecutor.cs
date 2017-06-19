using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http.OAuth;
using Raven.Client.Json.Converters;
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

        //Monitor.TryEnter/Monitor.Exit won't work here because we need a mutex without thread affinity
        private readonly SemaphoreSlim _updateTopologySemaphore = new SemaphoreSlim(1, 1);

        protected ConcurrentDictionary<ServerNode, NodeStatus> _failedNodesTimers = new ConcurrentDictionary<ServerNode, NodeStatus>();

        private readonly string _apiKey;
        private readonly string _databaseName;

        protected static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecutor>("Client");
        private DateTime _lastReturnedResponse;

        public readonly JsonContextPool ContextPool;

        public class AggressiveCacheOptions
        {
            public TimeSpan? Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly AsyncLocal<AggressiveCacheOptions> AggressiveCaching = new AsyncLocal<AggressiveCacheOptions>();

        public readonly HttpCache Cache = new HttpCache();

        public IReadOnlyList<ServerNode> TopologyNodes => _nodeSelector.Topology.Nodes;

        private Timer _updateTopologyTimer;

        private Timer _updateCurrentTokenTimer;

        protected NodeSelector _nodeSelector;

        private TimeSpan? _defaultTimeout;

        //note: the condition for non empty nodes is precaution, should never happen..
        public string Url => _nodeSelector?.GetCurrentNode()?.Url;

        public long TopologyEtag;

        protected bool _withoutTopology;

        public TimeSpan? DefaultTimeout
        {
            get => _defaultTimeout;
            set
            {
                if (value.HasValue && value.Value > GlobalHttpClientTimeout)
                    throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{value}'.");

                _defaultTimeout = value;
            }
        }

        protected RequestExecutor(string databaseName, string apiKey)
        {
            _databaseName = databaseName;
            _apiKey = apiKey;
            TopologyEtag = 0;

            _lastReturnedResponse = DateTime.UtcNow;

            ContextPool = new JsonContextPool();

        }

        public string ApiKey => _apiKey;

        public static RequestExecutor Create(string[] urls, string databaseName, string apiKey)
        {
            var executor = new RequestExecutor(databaseName, apiKey);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(urls);
            return executor;
        }

        public static RequestExecutor CreateForSingleNode(string url, string databaseName, string apiKey)
        {
            var executor = new RequestExecutor(databaseName, apiKey)
            {
                _nodeSelector = new NodeSelector(new Topology
                {
                    Etag = -1,
                    Nodes = new List<ServerNode>
                    {
                        new ServerNode
                        {
                            Database = databaseName,
                            Url = url
                        }
                    }
                }),
                TopologyEtag = -2,
                _withoutTopology = true
            };
            return executor;
        }

        public virtual async Task<bool> UpdateTopologyAsync(ServerNode node, int timeout)
        {
            if (_disposed)
                return false;

            //prevent double topology updates if execution takes too much time
            // --> in cases with transient issues
            var lockTaken = await _updateTopologySemaphore.WaitAsync(timeout).ConfigureAwait(false);
            if (lockTaken == false)
                return false;

            try
            {
                if (_disposed)
                    return false;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetTopologyCommand();

                    await ExecuteAsync(node, context, command, shouldRetry: false).ConfigureAwait(false);

                    var serverHash = ServerHash.GetServerHash(node.Url, _databaseName);

                    TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, command.Result, context);

                    if (_nodeSelector == null)
                        _nodeSelector = new NodeSelector(command.Result);

                    else if (_nodeSelector.OnUpdateTopology(command.Result))
                        DisposeAllFailedNodesTimers();

                    TopologyEtag = _nodeSelector.Topology.Etag;
                }
            }
            finally
            {
                _updateTopologySemaphore.Release();
            }
            return true;
        }

        protected void DisposeAllFailedNodesTimers()
        {
            var oldFailedNodesTimers = _failedNodesTimers;
            _failedNodesTimers.Clear();

            foreach (var failedNodesTimers in oldFailedNodesTimers)
            {
                failedNodesTimers.Value.Dispose();
            }

        }

        public void Execute<TResult>(RavenCommand<TResult> command, JsonOperationContext context, CancellationToken token = default(CancellationToken))
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context, token));
        }

        public async Task ExecuteAsync<TResult>(RavenCommand<TResult> command, JsonOperationContext context, CancellationToken token = default(CancellationToken))
        {
            var topologyUpdate = _firstTopologyUpdate;


            if (topologyUpdate != null && topologyUpdate.Status == TaskStatus.RanToCompletion || _withoutTopology)
            {
                await ExecuteAsync(_nodeSelector.GetCurrentNode(), context, command, token).ConfigureAwait(false);
                return;
            }
            await UnlikelyExecuteAsync(command, context, token, topologyUpdate).ConfigureAwait(false);
        }

        private async Task UnlikelyExecuteAsync<TResult>(RavenCommand<TResult> command, JsonOperationContext context, CancellationToken token, Task topologyUpdate)
        {
            try
            {
                if (topologyUpdate == null)
                {
                    lock (this)
                    {
                        if (_firstTopologyUpdate == null)
                        {
                            if (_lastKnownUrls == null)
                                throw new InvalidOperationException("No known topology and no previously known one, cannot proceed, likely a bug");
                            _firstTopologyUpdate = FirstTopologyUpdate(_lastKnownUrls);
                        }
                        topologyUpdate = _firstTopologyUpdate;
                    }
                }
                await topologyUpdate.ConfigureAwait(false);
            }
            catch (Exception)
            {
                lock (this)
                {
                    if (_firstTopologyUpdate == topologyUpdate)
                        _firstTopologyUpdate = null; // next request will raise it
                }
                throw;
            }

            await ExecuteAsync(_nodeSelector.GetCurrentNode(), context, command, token).ConfigureAwait(false);
        }

        private void UpdateTopologyCallback(object _)
        {
            var time = DateTime.UtcNow;
            if (time - _lastReturnedResponse <= TimeSpan.FromMinutes(5))
                return;

            var serverNode = _nodeSelector.GetCurrentNode();
            GC.KeepAlive(Task.Run(async () =>
            {
                try
                {
                    await UpdateTopologyAsync(serverNode, 0).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Couldn't Update Topology from _updateTopologyTimer task", e);
                }
            }));
        }

        protected async Task FirstTopologyUpdate(string[] initialUrls)
        {
            var list = new List<(string, Exception)>();
            foreach (var url in initialUrls)
            {
                try
                {
                    await UpdateTopologyAsync(new ServerNode
                        {
                            Url = url,
                            Database = _databaseName,
                        }, Timeout.Infinite)
                        .ConfigureAwait(false);

                    IntializeUpdateTopologyTimer();
                    return;
                }
                catch (Exception e)
                {
                    if (initialUrls.Length == 0)
                    {
                        _lastKnownUrls = initialUrls;
                        throw new InvalidOperationException("Cannot get topology from server: " + url, e);
                    }
                    list.Add((url, e));
                }
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                foreach (var url in initialUrls)
                {
                    if (TryLoadFromCache(url, context) == false)
                        continue;

                    IntializeUpdateTopologyTimer();
                    return;
                }
            }

            _lastKnownUrls = initialUrls;

            throw new AggregateException("Failed to retrieve cluster topology from all known nodes" + Environment.NewLine +
                                         string.Join(Environment.NewLine, list.Select(x => x.Item1 + " -> " + x.Item2?.Message))
                , list.Select(x => x.Item2));
        }

        private void IntializeUpdateTopologyTimer()
        {
            if (_updateTopologyTimer != null)
                return;

            lock (this)
            {
                if (_updateTopologyTimer != null)
                    return;

                _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
            }
        }

        protected virtual bool TryLoadFromCache(string url, JsonOperationContext context)
        {
            var serverHash = ServerHash.GetServerHash(url, _databaseName);
            var cachedTopology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, context);

            if (cachedTopology == null)
                return false;

            _nodeSelector = new NodeSelector(cachedTopology);
            TopologyEtag = -2;
            return true;
        }

        public async Task ExecuteAsync<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command, CancellationToken token = default(CancellationToken), bool shouldRetry = true)
        {
            var request = CreateRequest(chosenNode, command, out string url);

            var nodeIndex = _nodeSelector?.GetCurrentNodeIndex() ?? 0;

            using (var cachedItem = GetFromCache(context, command, request, url, out long cachedEtag, out BlittableJsonReaderObject cachedValue))
            {
                if (cachedEtag != 0)
                {
                    var aggressiveCacheOptions = AggressiveCaching.Value;
                    if (aggressiveCacheOptions != null && cachedItem.Age < aggressiveCacheOptions.Duration)
                    {
                        command.SetResponse(cachedValue, fromCache: true);
                        return;
                    }

                    request.Headers.TryAddWithoutValidation("If-None-Match", $"\"{cachedEtag}\"");
                }

                if (!_withoutTopology)
                    request.Headers.TryAddWithoutValidation("Topology-Etag", $"\"{TopologyEtag}\"");

                var sp = Stopwatch.StartNew();
                HttpResponseMessage response = null;
                try
                {
                    var client = GetHttpClientForCommand(command);
                    var timeout = command.Timeout ?? _defaultTimeout;
                    if (timeout.HasValue)
                    {
                        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, CancellationToken.None))
                        {
                            cts.CancelAfter(timeout.Value);
                            try
                            {
                                response = await command.SendAsync(client, request, cts.Token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException e)
                            {
                                if (cts.IsCancellationRequested && token.IsCancellationRequested == false) // only when we timed out
                                    throw new TimeoutException($"The request for {request.RequestUri} failed with timeout after {timeout}", e);
                                throw;
                            }
                        }
                    }
                    else
                    {
                        response = await command.SendAsync(client, request, token).ConfigureAwait(false);
                    }
                    sp.Stop();
                }
                catch (HttpRequestException e) // server down, network down
                {
                    sp.Stop();
                    if (shouldRetry == false)
                        throw;
                    if (await HandleServerDown(chosenNode, nodeIndex, context, command, request, response, e).ConfigureAwait(false) == false)
                        throw new AllTopologyNodesDownException("Tried to send request to all configured nodes in the topology, all of them seem to be down or not responding.", _nodeSelector.Topology, e);

                    return;
                }

                command.StatusCode = response.StatusCode;

                if (response.StatusCode == HttpStatusCode.NotModified)
                {
                    cachedItem.NotModified();

                    if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse(cachedValue, fromCache: true);

                    response.Dispose();

                    return;
                }
                if (response.IsSuccessStatusCode == false)
                {
                    if (await HandleUnsuccessfulResponse(chosenNode, nodeIndex, context, command, request, response, url).ConfigureAwait(false) == false)
                    {
                        if (command.FailedNodes.Count == 0) //precaution, should never happen at this point
                            throw new InvalidOperationException("Received unsuccessful response and couldn't recover from it. Also, no record of exceptions per failed nodes. This is weird and should not happen.");

                        if (command.FailedNodes.Count == 1)
                        {
                            var node = command.FailedNodes.First();
                            throw new UnsuccessfulRequestException(node.Key.Url, node.Value);
                        }

                        throw new AllTopologyNodesDownException("Received unsuccessful response from all servers and couldn't recover from it.",
                            new AggregateException(command.FailedNodes.Select(x => new UnsuccessfulRequestException(x.Key.Url, x.Value))));
                    }
                    return; // we either handled this already in the unsuccessful response or we are throwing
                }
                await command.ProcessResponse(context, Cache, response, url).ConfigureAwait(false);
                _lastReturnedResponse = DateTime.UtcNow;
                if (command.RefreshTopology)
                {
                    await UpdateTopologyAsync(new ServerNode
                    {
                        Url = url,
                        Database = _databaseName,
                    }, 0).ConfigureAwait(false);
                }
            }
        }

        private HttpCache.ReleaseCacheItem GetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, string url, out long cachedEtag, out BlittableJsonReaderObject cachedValue)
        {
            if (command.IsReadRequest && command.ResponseType == RavenCommandResponseType.Object)
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

            if (node.ClusterToken != null)
                request.Headers.Add("Raven-Authorization", node.ClusterToken);

            if (!request.Headers.Contains("Raven-Client-Version"))
                request.Headers.Add("Raven-Client-Version", ClientVersion);
            return request;
        }

        public event Action<StringBuilder> AdditionalErrorInformation;

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ServerNode chosenNode, int nodeIndex, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, string url)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    if (command.ResponseType == RavenCommandResponseType.Empty)
                        return true;
                    else if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse(null, fromCache: false);
                    else
                        command.SetResponseRaw(response, null, context);
                    return true;
                case HttpStatusCode.Unauthorized:
                case HttpStatusCode.PreconditionFailed:
                    if (string.IsNullOrEmpty(_apiKey))
                        throw AuthorizationException.EmptyApiKey(url);
                    if (++command.AuthenticationRetries > 1)
                        throw AuthorizationException.Unauthorized(url);


#if DEBUG && FIDDLER
// Make sure to avoid a cross DNS security issue, when running with Fiddler
                if (string.IsNullOrEmpty(oauthSource) == false)
                    oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

                    await HandleUnauthorized(chosenNode, context).ConfigureAwait(false);
                    await ExecuteAsync(chosenNode, context, command).ConfigureAwait(false);
                    return true;
                case HttpStatusCode.Forbidden:
                    throw AuthorizationException.Forbidden(url);
                case HttpStatusCode.GatewayTimeout:
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    await HandleServerDown(chosenNode, nodeIndex, context, command, request, response, null).ConfigureAwait(false);
                    break;
                case HttpStatusCode.Conflict:
                    await HandleConflict(context, response).ConfigureAwait(false);
                    break;
                default:
                    await ExceptionDispatcher.Throw(context, response, AdditionalErrorInformation).ConfigureAwait(false);
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

        public static async Task<Stream> ReadAsStreamUncompressedAsync(HttpResponseMessage response)
        {
            var serverStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var stream = serverStream;
            var encoding = response.Content.Headers.ContentEncoding.FirstOrDefault();
            if (encoding != null && encoding.Contains("gzip"))
                return new GZipStream(stream, CompressionMode.Decompress);
            if (encoding != null && encoding.Contains("deflate"))
                return new DeflateStream(stream, CompressionMode.Decompress);

            return serverStream;
        }

        private async Task<bool> HandleServerDown<TResult>(ServerNode chosenNode, int nodeIndex, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, HttpRequestException e)
        {
            if (command.FailedNodes == null)
                command.FailedNodes = new Dictionary<ServerNode, ExceptionDispatcher.ExceptionSchema>();

            await AddFailedResponseToCommand(chosenNode, context, command, request, response, e).ConfigureAwait(false);

            var nodeSelector = _nodeSelector;

            SpawnHealthChecks(chosenNode, nodeIndex);

            nodeSelector?.OnFailedRequest(nodeIndex);
            var currentNode = nodeSelector?.GetCurrentNode();

            if (nodeSelector == null || command.FailedNodes.ContainsKey(currentNode))
                return false; //we tried all the nodes...nothing left to do

            await ExecuteAsync(currentNode, context, command).ConfigureAwait(false);

            return true;
        }

        private void SpawnHealthChecks(ServerNode chosenNode, int nodeIndex)
        {
            var nodeStatus = new NodeStatus(this, nodeIndex, chosenNode);
            if (_failedNodesTimers.TryAdd(chosenNode, nodeStatus))
                nodeStatus.StartTimer();
        }

        private async Task CheckNodeStatusCallback(NodeStatus nodeStatus)
        {
            var copy = TopologyNodes;
            if (nodeStatus.NodeIndex >= copy.Count)
                return; // topology index changed / removed
            var serverNode = copy[nodeStatus.NodeIndex];
            if (ReferenceEquals(serverNode, nodeStatus.Node) == false)
                return; // topology changed, nothing to check

            try
            {
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    NodeStatus status;
                    try
                    {
                        await PerformHealthCheck(serverNode, context).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"{serverNode.ClusterTag} is still down", e);

                        if (_failedNodesTimers.TryGetValue(nodeStatus.Node, out status))
                            nodeStatus.UpdateTimer();

                        return;// will wait for the next timer call
                    }

                    if (_failedNodesTimers.TryRemove(nodeStatus.Node, out status))
                    {
                        status.Dispose();
                    }
                    _nodeSelector.RestoreNodeIndex(nodeStatus.NodeIndex);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to check node topology, will ignore this node until next topology update", e);
            }
        }

        protected virtual async Task PerformHealthCheck(ServerNode serverNode, JsonOperationContext context)
        {
            await ExecuteAsync(serverNode, context, new GetStatisticsCommand(debugTag: "failure=check"), shouldRetry: false).ConfigureAwait(false);
        }

        private static async Task AddFailedResponseToCommand<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, HttpRequestException e)
        {
            if (response != null)
            {
                var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                var ms = new MemoryStream();
                await stream.CopyToAsync(ms).ConfigureAwait(false);
                try
                {
                    ms.Position = 0;
                    using (var responseJson = context.ReadForMemory(ms, "RequestExecutor/HandleServerDown/ReadResponseContent"))
                    {
                        command.FailedNodes.Add(chosenNode, JsonDeserializationClient.ExceptionSchema(responseJson));
                    }
                }
                catch
                {
                    // we failed to parse the error
                    ms.Position = 0;
                    command.FailedNodes.Add(chosenNode, new ExceptionDispatcher.ExceptionSchema
                    {
                        Url = request.RequestUri.ToString(),
                        Message = "Got unrecognized response from the server",
                        Error = new StreamReader(ms).ReadToEnd(),
                        Type = "Unparseable Server Response"
                    });
                }
                return;
            }
            //this would be connections that didn't have response, such as "couldn't connect to remote server"
            command.FailedNodes.Add(chosenNode, new ExceptionDispatcher.ExceptionSchema
            {
                Url = request.RequestUri.ToString(),
                Message = e.Message,
                Error = e.ToString(),
                Type = e.GetType().FullName
            });
        }

        private static void ThrowEmptyTopology()
        {
            throw new InvalidOperationException("Empty database topology, this shouldn't happen.");
        }

        public async Task<string> GetAuthenticationToken(JsonOperationContext context, ServerNode node)
        {
            return node.ClusterToken = await _authenticator.GetAuthenticationTokenAsync(_apiKey, node.Url, context).ConfigureAwait(false);          
        }

        private async Task HandleUnauthorized(ServerNode node, JsonOperationContext context, bool shouldThrow = true)
        {
            try
            {
                var currentToken = await _authenticator.GetAuthenticationTokenAsync(_apiKey, node.Url, context).ConfigureAwait(false);
                node.ClusterToken = currentToken;
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
                var topology = _nodeSelector.Topology;
                foreach (var node in topology.Nodes)
                {
#pragma warning disable 4014
                    HandleUnauthorized(node, context, shouldThrow: false);
#pragma warning restore 4014
                }
            }
        }

        protected bool _disposed;
        protected Task _firstTopologyUpdate;
        protected string[] _lastKnownUrls;

        public virtual void Dispose()
        {
            if (_disposed)
                return;

            _updateTopologySemaphore.Wait();

            if (_disposed)
                return;
            _disposed = true;
            Cache.Dispose();
            ContextPool.Dispose();
            _updateCurrentTokenTimer?.Dispose();
            _updateTopologyTimer?.Dispose();
            DisposeAllFailedNodesTimers();
            // shared instance, cannot dispose!
            //_httpClient.Dispose();
        }

        private static HttpClient CreateClient(TimeSpan timeout)
        {
            var httpMessageHandler = new HttpClientHandler();
            httpMessageHandler.ServerCertificateCustomValidationCallback += OnServerCertificateCustomValidationCallback;

            return new HttpClient(httpMessageHandler)
            {
                Timeout = timeout
            };
        }

        public static event Func<HttpRequestMessage, X509Certificate2, X509Chain, SslPolicyErrors, bool> ServerCertificateCustomValidationCallback;

        private static bool OnServerCertificateCustomValidationCallback(HttpRequestMessage msg, X509Certificate2 cert, X509Chain chain, SslPolicyErrors errors)
        {
            var onServerCertificateCustomValidationCallback = ServerCertificateCustomValidationCallback;
            if (onServerCertificateCustomValidationCallback == null)
                return errors == SslPolicyErrors.None;
            return onServerCertificateCustomValidationCallback(msg, cert, chain, errors);
        }

        private static HttpClient GetHttpClientForCommand<T>(RavenCommand<T> command)
        {
            var timeout = command.Timeout;
            if (timeout.HasValue && timeout > GlobalHttpClientTimeout)
                throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{timeout}'.");

            return GlobalHttpClient.Value;
        }

        public class NodeStatus : IDisposable
        {
            private TimeSpan _timerPeriod;
            private readonly RequestExecutor _requestExecutor;
            public readonly int NodeIndex;
            public readonly ServerNode Node;
            private Timer _timer;

            public NodeStatus(RequestExecutor requestExecutor, int nodeIndex, ServerNode node)
            {
                _requestExecutor = requestExecutor;
                NodeIndex = nodeIndex;
                Node = node;
                _timerPeriod = TimeSpan.FromMilliseconds(100);
            }

            private TimeSpan NextTimerPeriod()
            {
                if (_timerPeriod >= TimeSpan.FromSeconds(5))
                    return TimeSpan.FromSeconds(5);

                _timerPeriod += TimeSpan.FromMilliseconds(100);
                return _timerPeriod;
            }

            public void StartTimer()
            {
                _timer = new Timer(TimerCallback, null, _timerPeriod, Timeout.InfiniteTimeSpan);
            }

            private void TimerCallback(object state)
            {
                GC.KeepAlive(_requestExecutor.CheckNodeStatusCallback(this));
            }

            public void UpdateTimer()
            {
                Debug.Assert(_timer != null);
                _timer.Change(NextTimerPeriod(), Timeout.InfiniteTimeSpan);
            }

            public void Dispose()
            {
                _timer?.Dispose();
            }
        }

        public class NodeSelector
        {
            private Topology _topology;

            public Topology Topology => _topology;

            private int _currentNodeIndex;

            public NodeSelector(Topology topology)
            {
                _topology = topology;
            }

            public int GetCurrentNodeIndex()
            {
                return _currentNodeIndex;
            }

            public void OnFailedRequest(int nodeIndex)
            {
                if (Topology.Nodes.Count == 0)
                    ThrowEmptyTopology();

                var nextNodeIndex = nodeIndex < Topology.Nodes.Count - 1 ? nodeIndex + 1 : 0;
                Interlocked.CompareExchange(ref _currentNodeIndex, nextNodeIndex, nodeIndex);
            }

            public bool OnUpdateTopology(Topology topology, bool forceUpdate = false)
            {
                if (topology == null)
                    return false;

                var oldTopology = _topology;
                do
                {
                    if (oldTopology.Etag >= topology.Etag && forceUpdate == false)
                        return false;

                    if (forceUpdate == false)
                    {
                        Interlocked.Exchange(ref _currentNodeIndex, 0);
                    }

                    var changed = Interlocked.CompareExchange(ref _topology, topology, oldTopology);
                    if (changed == oldTopology)
                        return true;
                    oldTopology = changed;
                } while (true);
            }

            public ServerNode GetCurrentNode()
            {
                if (Topology.Nodes.Count == 0)
                    ThrowEmptyTopology();
                return Topology.Nodes[_currentNodeIndex];
            }

            public void RestoreNodeIndex(int nodeIndex)
            {
                var currentNodeIndex = _currentNodeIndex;
                while(currentNodeIndex > nodeIndex)
                {
                    var result = Interlocked.CompareExchange(ref _currentNodeIndex, nodeIndex, currentNodeIndex);
                    if (result == currentNodeIndex)
                        return;
                    currentNodeIndex = result;
                }
            }
        }

        public async Task<ServerNode> GetCurrentNode()
        {
            if (_firstTopologyUpdate.Status != TaskStatus.RanToCompletion)
                await _firstTopologyUpdate.ConfigureAwait(false);

            return _nodeSelector.GetCurrentNode();
        }
    }
}