using System;
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
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http.OAuth;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Commands;
using Raven.Client.Util;
using Sparrow;
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
        private readonly AsyncManualResetEvent _firstTimeTopologyUpdateFlag = new AsyncManualResetEvent();
        private readonly string _initialUrl;
        private readonly string _apiKey;
        private readonly bool _requiresTopologyUpdates;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecutor>("Client");

        public readonly JsonContextPool ContextPool;

        public class AggressiveCacheOptions
        {
            public TimeSpan? Duration;
        }

        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();

        public readonly AsyncLocal<AggressiveCacheOptions> AggressiveCaching = new AsyncLocal<AggressiveCacheOptions>();

        public readonly HttpCache Cache = new HttpCache();

        private int _hasUpdatedTopologyAtLeastOnce;

        public IReadOnlyList<ServerNode> TopologyNodes => _nodeSelector.Topology.Nodes;

        private readonly Timer _updateTopologyTimer;

        private Timer _updateCurrentTokenTimer;

        public readonly NodeSelector _nodeSelector;

        private bool _disposed;
        private TimeSpan? _defaultTimeout;

        //note: the condition for non empty nodes is precaution, should never happen..
        public string Url => _nodeSelector.CurrentNode?.Url;

        public bool HasUpdatedTopologyOnce { get; private set; }

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

        private RequestExecutor(string url, string databaseName, string apiKey, bool requiresTopologyUpdates)
        {
            _initialUrl = url;
            _apiKey = apiKey;
            _requiresTopologyUpdates = requiresTopologyUpdates;
            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = new List<ServerNode>
                {
                    new ServerNode
                    {
                        Url = url,
                        Database = databaseName,
                        ClusterTag = "N/A" //initial topology before an update, so cluster token is not applicable here...
                    }
                },
                Etag = int.MinValue,
                SLA = new TopologySla
                {
                    RequestTimeThresholdInMilliseconds = 100
                }
            });

            ContextPool = new JsonContextPool();

            if (requiresTopologyUpdates == false)
                return;

            _updateTopologyTimer = new Timer(UpdateTopologyCallback, null, 0, Timeout.Infinite);

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var node = _nodeSelector.CurrentNode;
                var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

                var cachedTopology = TopologyLocalCache.TryLoadTopologyFromLocalCache(serverHash, context);
                if (cachedTopology != null && cachedTopology.Etag > _nodeSelector.Topology.Etag)
                {
                    _nodeSelector.OnUpdateTopology(cachedTopology);
                }
            }
        }

        public string ApiKey => _apiKey;

        internal int CurrentNodeIndex => _nodeSelector.CurrentNodeIndex;

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
            GC.KeepAlive(UpdateTopologyAsync());
        }

        public async Task<bool> UpdateTopologyAsync()
        {
            if (_disposed)
                return false;

            //prevent double topology updates if execution takes too much time
            // --> in cases with transient issues
            var lockTaken = _updateTopologySemaphore.Wait(0);
            if (lockTaken == false)
                return false;
            try
            {
                if (_disposed)
                    return false;

                JsonOperationContext context;
                var operationContext = ContextPool.AllocateOperationContext(out context);
                try
                {
                    var node = _nodeSelector.CurrentNode;
                    var serverHash = ServerHash.GetServerHash(node.Url, node.Database);

                    var command = new GetTopologyCommand();
                    try
                    {
                        await ExecuteAsync(node, context, command, shouldRetry: false);

                        if (command.Result.Nodes.Count > 0 && _nodeSelector.OnUpdateTopology(command.Result))
                        {
                            OnTopologyChange();
                            TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _nodeSelector.Topology, context);
                        }
                    }
                    catch (HttpRequestException e)
                    {
                        command.FailedNodes.Add(node, null);

                        _nodeSelector.OnFailedRequest();
                        var errors = new List<Exception> { e };
                        while (command.FailedNodes.ContainsKey(_nodeSelector.CurrentNode) == false)
                        {
                            try
                            {
                                await ExecuteAsync(_nodeSelector.CurrentNode, context, command, shouldRetry: false);
                            }
                            catch (Exception e2)
                            {
                                errors.Add(e2);
                                _nodeSelector.OnFailedRequest();
                                continue;
                            }

                            //because we failed to contact a server, force update topology because it is likely that the etag 
                            //hadn't changed, since the topology itself didn't change, it's a node that went down
                            if (command.Result.Nodes.Count > 0 && _nodeSelector.OnUpdateTopology(command.Result, forceUpdate: true))
                            {
                                OnTopologyChange();
                                TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, _nodeSelector.Topology, context);
                                return true;
                            }
                        }

                        //if we are here, we went through all nodes and failed everywhere
                        throw new AggregateException("Tried to update topology from all nodes but failed. Cannot continue.", errors);
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
                _updateTopologySemaphore.Release();
            }
            return true;
        }

        public void Execute<TResult>(RavenCommand<TResult> command, JsonOperationContext context, CancellationToken token = default(CancellationToken))
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context, token));
        }

        public async Task ExecuteAsync<TResult>(RavenCommand<TResult> command, JsonOperationContext context, CancellationToken token = default(CancellationToken))
        {
            if (Interlocked.CompareExchange(ref _hasUpdatedTopologyAtLeastOnce, 1, 0) == 0)
            {
                await RunTopologyUpdateForFirstTimeIfRelevant();
            }

            if (!_firstTimeTopologyUpdateFlag.IsSet)
            {
                await _firstTimeTopologyUpdateFlag.WaitAsync();
            }

            await ExecuteAsync(_nodeSelector.CurrentNode, context, command, token).ConfigureAwait(false);
        }

        private async Task RunTopologyUpdateForFirstTimeIfRelevant()
        {
            if (_requiresTopologyUpdates)
            {
                try
                {
                    if (await UpdateTopologyAsync())
                        _nodeSelector.TrySetToUrl(_initialUrl);
                }
                finally
                {
                    _firstTimeTopologyUpdateFlag.Set();
                }
            }
            else
            {
                _firstTimeTopologyUpdateFlag.Set(); //no topology updates --> no need to wait for first topology updates
            }
        }

        public async Task ExecuteAsync<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command, CancellationToken token = default(CancellationToken), bool shouldRetry = true)
        {
            string url;
            var request = CreateRequest(chosenNode, command, out url);

            long cachedEtag;
            BlittableJsonReaderObject cachedValue;
            HttpCache.ReleaseCacheItem cachedItem;
            using (cachedItem = GetFromCache(context, command, request, url, out cachedEtag, out cachedValue))
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

                    if (await HandleServerDown(chosenNode, context, command, request, response, e) == false)
                        throw new AllTopologyNodesDownException("Tried to send request to all configured nodes in the topology, all of them seem to be down or not responding.", _nodeSelector.Topology, e);

                    return;
                }

                command.StatusCode = response.StatusCode;

                if (response.StatusCode == HttpStatusCode.NotModified)
                {
                    cachedItem.NotModified();

                    if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse(cachedValue, fromCache: true);

                    return;
                }
                if (response.IsSuccessStatusCode == false)
                {
                    if (await HandleUnsuccessfulResponse(chosenNode, context, command, request, response, url).ConfigureAwait(false) == false)
                    {
                        if (command.FailedNodes.Count == 0) //precaution, should never happen at this point
                            throw new InvalidOperationException("Received unsuccessful response and couldn't recover from it. Also, no record of exceptions per failed nodes. This is weird and should not happen.");

                        throw new AllTopologyNodesDownException("Received unsuccessful response and couldn't recover from it.",
                            new AggregateException(command.FailedNodes.Select(x => new UnsuccessfulRequestException(x.Key.Url, x.Value))));
                    }
                    return; // we either handled this already in the unsuccessful response or we are throwing
                }
                await command.ProcessResponse(context, Cache, response, url).ConfigureAwait(false);
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

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, string url)
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
                    await HandleServerDown(chosenNode, context, command, request, response, null).ConfigureAwait(false);
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

        private async Task<bool> HandleServerDown<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, HttpRequestException e)
        {
            if (command.FailedNodes == null)
                command.FailedNodes = new Dictionary<ServerNode, ExceptionDispatcher.ExceptionSchema>();

            chosenNode.IsFailed = true;
            await AddFailedResponseToCommand(chosenNode, context, command, request, response, e);

            _nodeSelector.OnFailedRequest();
            if (command.FailedNodes.ContainsKey(_nodeSelector.CurrentNode)) //we tried all the nodes...nothing left to do
                return false;

            await ExecuteAsync(_nodeSelector.CurrentNode, context, command);

            return true;
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
                        Type = "Unparsable Server Response"
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

        public string UrlFor(string documentId)
        {
            var node = _nodeSelector.CurrentNode;
            return $"{node.Url}/databases/{node.Database}/docs?id={documentId}";
        }       

        private static void ThrowEmptyTopology()
        {
            throw new InvalidOperationException("Empty database topology, this shouldn't happen.");
        }

        public async Task<string> GetAuthenticationToken(JsonOperationContext context, ServerNode node)
        {
            return await _authenticator.GetAuthenticationTokenAsync(_apiKey, node.Url, context).ConfigureAwait(false);
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

        public void Dispose()
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
            _updateTopologySemaphore.Dispose();
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

        protected void OnTopologyChange()
        {
            HasUpdatedTopologyOnce = true;
        }

        public class NodeSelector
        {
            private Topology _topology;

            public Topology Topology => _topology;

            public int CurrentNodeIndex { get; private set; }

            public NodeSelector(Topology topology)
            {
                _topology = topology;
            }

            public ServerNode CurrentNode => _topology.Nodes.Count > 0 ? _topology.Nodes[CurrentNodeIndex] : null;

            public void OnFailedRequest()
            {
                if (Topology.Nodes.Count == 0)
                    ThrowEmptyTopology();

                if (CurrentNodeIndex < 0) //precaution, should never happen
                    CurrentNodeIndex = 0;

                CurrentNodeIndex = CurrentNodeIndex < Topology.Nodes.Count - 1 ? CurrentNodeIndex + 1 : 0;
            }

            public bool TrySetToUrl(string url)
            {
                var index = _topology.Nodes.FindIndex(node => node.Url.Equals(url, StringComparison.OrdinalIgnoreCase));
                if (index != -1)
                {
                    CurrentNodeIndex = index;
                    return true;
                }

                return false;
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
                        CurrentNodeIndex = 0;
                    }

                    var changed = Interlocked.CompareExchange(ref _topology, topology, oldTopology);
                    if (changed == oldTopology)
                        return true;
                    oldTopology = changed;
                } while (true);
            }
        }
    }
}