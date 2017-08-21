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
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Client.Http
{
    public class RequestExecutor : IDisposable
    {
        // https://aspnetmonsters.com/2016/08/2016-08-27-httpclientwrong/

        private static readonly TimeSpan GlobalHttpClientTimeout = TimeSpan.FromHours(12);

        private static readonly ConcurrentDictionary<string, Lazy<HttpClient>> GlobalHttpClient = new ConcurrentDictionary<string, Lazy<HttpClient>>();

        private readonly SemaphoreSlim _updateTopologySemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _updateClientConfigurationSemaphore = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<ServerNode, NodeStatus> _failedNodesTimers = new ConcurrentDictionary<ServerNode, NodeStatus>();

        public X509Certificate2 Certificate { get; }
        private readonly string _databaseName;

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecutor>("Client");
        private DateTime _lastReturnedResponse;

        protected readonly ReadBalanceBehavior _readBalanceBehavior;

        public readonly JsonContextPool ContextPool;

        public readonly AsyncLocal<AggressiveCacheOptions> AggressiveCaching = new AsyncLocal<AggressiveCacheOptions>();

        public readonly HttpCache Cache = new HttpCache();

        public Topology Topology => _nodeSelector?.Topology;

        public IReadOnlyList<ServerNode> TopologyNodes => _nodeSelector?.Topology.Nodes;

        private Timer _updateTopologyTimer;

        protected NodeSelector _nodeSelector;

        private TimeSpan? _defaultTimeout;

        public long NumberOfServerRequests;

        //note: the condition for non empty nodes is precaution, should never happen..
        public string Url
        {
            get
            {
                if (_nodeSelector == null)
                    return null;

                var (_, currentNode) = _nodeSelector.GetPreferredNode();

                return currentNode?.Url;
            }
        }

        public long TopologyEtag { get; protected set; }

        public long ClientConfigurationEtag { get; internal set; }

        public readonly DocumentConventions Conventions;

        protected bool _disableTopologyUpdates;

        protected bool _disableClientConfigurationUpdates;

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

        public event EventHandler<(long RaftCommandIndex, ClientConfiguration Configuration)> ClientConfigurationChanged;

        public event Action<string, Exception> FailedRequest;
        public event Action<Topology> TopologyUpdated;

        private void OnFailedRequest(string url, Exception e)
        {
            FailedRequest?.Invoke(url, e);
        }

        protected RequestExecutor(string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            _readBalanceBehavior = conventions.ReadBalanceBehavior;
            _databaseName = databaseName;
            Certificate = certificate;

            _lastReturnedResponse = DateTime.UtcNow;

            ContextPool = new JsonContextPool();
            Conventions = conventions.Clone();


            string thumbprint = string.Empty;
            if (certificate != null)
                thumbprint = certificate.Thumbprint;


            if (GlobalHttpClient.TryGetValue(thumbprint, out var lazyClient) == false)
            {
                lazyClient = GlobalHttpClient.GetOrAdd(thumbprint, new Lazy<HttpClient>(CreateClient));
            }

            _httpClient = lazyClient.Value;
        }

        public static RequestExecutor Create(string[] urls, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            var executor = new RequestExecutor(databaseName, certificate, conventions);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(urls);
            return executor;
        }

        public static RequestExecutor CreateForSingleNodeWithConfigurationUpdates(string url, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            var executor = CreateForSingleNodeWithoutConfigurationUpdates(url, databaseName, certificate, conventions);
            executor._disableClientConfigurationUpdates = false;

            return executor;
        }

        public static RequestExecutor CreateForSingleNodeWithoutConfigurationUpdates(string url, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            ValidateUrls(new[] { url }, certificate);
            var executor = new RequestExecutor(databaseName, certificate, conventions)
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
                _disableTopologyUpdates = true,
                _disableClientConfigurationUpdates = true
            };
            return executor;
        }

        protected virtual async Task UpdateClientConfigurationAsync()
        {
            if (_disposed)
                return;

            await _updateClientConfigurationSemaphore.WaitAsync().ConfigureAwait(false);

            var oldDisableClientConfigurationUpdates = _disableClientConfigurationUpdates;
            _disableClientConfigurationUpdates = true;

            try
            {
                if (_disposed)
                    return;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetClientConfigurationOperation.GetClientConfigurationCommand();

                    var (currentIndex, currentNode) = ChooseNodeForRequest(command, 0);
                    await ExecuteAsync(currentNode, currentIndex, context, command, shouldRetry: false).ConfigureAwait(false);

                    var result = command.Result;
                    if (result == null)
                        return;

                    Conventions.UpdateFrom(result.Configuration);
                    ClientConfigurationEtag = result.Etag;
                    ClientConfigurationChanged?.Invoke(this, (result.Etag, result.Configuration));
                }
            }
            finally
            {
                _disableClientConfigurationUpdates = oldDisableClientConfigurationUpdates;
                _updateClientConfigurationSemaphore.Release();
            }
        }

        public virtual async Task<bool> UpdateTopologyAsync(ServerNode node, int timeout, bool forceUpdate = false)
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

                    await ExecuteAsync(node, null, context, command, shouldRetry: false).ConfigureAwait(false);

                    var serverHash = ServerHash.GetServerHash(node.Url, _databaseName);

                    TopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, command.Result, context);

                    if (_nodeSelector == null)
                    {
                        _nodeSelector = new NodeSelector(command.Result);

                        if (_readBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }
                    else if (_nodeSelector.OnUpdateTopology(command.Result, forceUpdate: forceUpdate))
                    {
                        DisposeAllFailedNodesTimers();
                        if (_readBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }

                    TopologyEtag = _nodeSelector.Topology.Etag;
                    OnTopologyUpdated(command.Result);
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

        public void Execute<TResult>(RavenCommand<TResult> command,
            JsonOperationContext context,
            CancellationToken token = default(CancellationToken),
            int? sessionId = null)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context, token, sessionId));
        }

        public Task ExecuteAsync<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            CancellationToken token = default(CancellationToken),
            int? sessionId = null)
        {
            var topologyUpdate = _firstTopologyUpdate;

            if (topologyUpdate != null && topologyUpdate.Status == TaskStatus.RanToCompletion || _disableTopologyUpdates)
            {
                var (nodeIndex, chosenNode) = ChooseNodeForRequest(command, sessionId ?? 0);
                return ExecuteAsync(chosenNode, nodeIndex, context, command, token, sessionId: sessionId);
            }

            return UnlikelyExecuteAsync(command, context, token, topologyUpdate, sessionId);
        }

        public (int CurrentIndex, ServerNode CurrentNode) ChooseNodeForRequest<TResult>(RavenCommand<TResult> cmd, int sessionId)
        {
            if (cmd.IsReadRequest == false)
                return _nodeSelector.GetPreferredNode();

            switch (_readBalanceBehavior)
            {
                case ReadBalanceBehavior.None:
                    return _nodeSelector.GetPreferredNode();
                case ReadBalanceBehavior.RoundRobin:
                    return _nodeSelector.GetNodeBySessionId(sessionId);
                case ReadBalanceBehavior.FastestNode:
                    return _nodeSelector.GetFastestNode();
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task UnlikelyExecuteAsync<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            CancellationToken token,
            Task topologyUpdate,
            int? sessionId = null)
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

            var (currentIndex, currentNode) = ChooseNodeForRequest(command, sessionId ?? 0);
            await ExecuteAsync(currentNode, currentIndex, context, command, token, true, sessionId).ConfigureAwait(false);
        }

        private void UpdateTopologyCallback(object _)
        {
            var time = DateTime.UtcNow;
            if (time - _lastReturnedResponse <= TimeSpan.FromMinutes(5))
                return;

            var (_, serverNode) = _nodeSelector.GetPreferredNode();
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
            ValidateUrls(initialUrls, Certificate);

            var list = new List<(string, Exception)>();
            foreach (var url in initialUrls)
            {
                try
                {
                    await UpdateTopologyAsync(new ServerNode
                        {
                            Url = url,
                            Database = _databaseName
                        }, Timeout.Infinite)
                        .ConfigureAwait(false);

                    InitializeUpdateTopologyTimer();
                    return;
                }
                catch (AuthorizationException)
                {
                    // auth exceptions will always happen, on all nodes
                    // so errors immediately
                    throw;
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

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = TopologyNodes?.ToList() ?? initialUrls.Select(url => new ServerNode
                {
                    Url = url,
                    Database = _databaseName,
                    ClusterTag = "!"
                }).ToList(),
                Etag = TopologyEtag
            });

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                foreach (var url in initialUrls)
                {
                    if (TryLoadFromCache(url, context) == false)
                        continue;

                    InitializeUpdateTopologyTimer();
                    return;
                }
            }

            _lastKnownUrls = initialUrls;

            throw new AggregateException("Failed to retrieve cluster topology from all known nodes" + Environment.NewLine +
                                         string.Join(Environment.NewLine, list.Select(x => x.Item1 + " -> " + x.Item2?.Message))
                , list.Select(x => x.Item2));
        }

        protected static void ValidateUrls(string[] initialUrls, X509Certificate2 certificate)
        {
            var requireHttps = certificate != null;
            foreach (var url in initialUrls)
            {
                if (Uri.TryCreate(url, UriKind.Absolute, out var uri) == false)
                    throw new InvalidOperationException("The url '" + url + "' is not valid");

                requireHttps |= string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase);
            }

            if (requireHttps == false)
                return;

            foreach (var url in initialUrls)
            {
                var uri = new Uri(url); // verified it works above

                if (string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (certificate != null)
                    throw new InvalidOperationException("The url " + url + " is using HTTP, but a certificate is specified, which require us to use HTTPS");

                throw new InvalidOperationException("The url " + url + " is using HTTP, but other urls are using HTTPS, and mixing of HTTP and HTTPS is not allowed");
            }
        }

        private void InitializeUpdateTopologyTimer()
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

        public async Task ExecuteAsync<TResult>(
            ServerNode chosenNode,
            int? nodeIndex,
            JsonOperationContext context,
            RavenCommand<TResult> command,
            CancellationToken token = default(CancellationToken),
            bool shouldRetry = true,
            int? sessionId = null)
        {
            var request = CreateRequest(context, chosenNode, command, out string url);

            using (var cachedItem = GetFromCache(context, command, url, out string cachedChangeVector, out BlittableJsonReaderObject cachedValue))
            {
                if (cachedChangeVector != null)
                {
                    var aggressiveCacheOptions = AggressiveCaching.Value;
                    if (aggressiveCacheOptions != null && 
                        cachedItem.Age < aggressiveCacheOptions.Duration && 
                        cachedItem.MightHaveBeenModified == false && 
                        command.AggressiveCacheAllowed)
                    {
                        command.SetResponse(cachedValue, fromCache: true);
                        return;
                    }

                    request.Headers.TryAddWithoutValidation("If-None-Match", $"\"{cachedChangeVector}\"");
                }

                if (_disableClientConfigurationUpdates == false)
                    request.Headers.TryAddWithoutValidation(Constants.Headers.ClientConfigurationEtag, $"\"{ClientConfigurationEtag}\"");

                if (_disableTopologyUpdates == false)
                    request.Headers.TryAddWithoutValidation(Constants.Headers.TopologyEtag, $"\"{TopologyEtag}\"");

                var sp = Stopwatch.StartNew();
                HttpResponseMessage response = null;
                var responseDispose = ResponseDisposeHandling.Automatic;
                try
                {
                    Interlocked.Increment(ref NumberOfServerRequests);
                    var timeout = command.Timeout ?? _defaultTimeout;
                    if (timeout.HasValue)
                    {
                        if (timeout > GlobalHttpClientTimeout)
                            ThrowTimeoutTooLarge(timeout);
                        
                        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, CancellationToken.None))
                        {
                            cts.CancelAfter(timeout.Value);
                            try
                            {
                                var preferredTask = command.SendAsync(_httpClient, request, cts.Token);
                                if (ShouldExecuteOnAll(chosenNode, command))
                                {
                                    await ExecuteOnAllToFigureOutTheFastest(chosenNode, command, preferredTask, cts.Token).ConfigureAwait(false);
                                }

                                response = await preferredTask.ConfigureAwait(false);
                            }
                            catch (OperationCanceledException e)
                            {
                                if (cts.IsCancellationRequested && token.IsCancellationRequested == false) // only when we timed out
                                {
                                    var timeoutException = new TimeoutException($"The request for {request.RequestUri} failed with timeout after {timeout}", e);
                                    if (shouldRetry == false)
                                        throw timeoutException;

                                    sp.Stop();
                                    if (await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, e).ConfigureAwait(false) == false)
                                        throw new AllTopologyNodesDownException($"Tried to send '{command.GetType().Name}' via `{request.Method} {request.RequestUri.PathAndQuery}` to all configured nodes in the topology: " + string.Join(",", _nodeSelector?.Topology.Nodes.Select(x => x.Url) ?? new string[0]), _nodeSelector?.Topology, timeoutException);

                                    return;
                                }
                                throw;
                            }
                        }
                    }
                    else
                    {
                        var preferredTask = command.SendAsync(_httpClient, request, token);
                        if (ShouldExecuteOnAll(chosenNode, command))
                        {
                            await ExecuteOnAllToFigureOutTheFastest(chosenNode, command, preferredTask, token).ConfigureAwait(false);
                        }

                        response = await preferredTask.ConfigureAwait(false);
                    }
                    sp.Stop();
                }
                catch (HttpRequestException e) // server down, network down
                {
                    if (shouldRetry == false)
                        throw;
                    sp.Stop();
                    if (await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, e).ConfigureAwait(false) == false)
                    {
                        throw new AllTopologyNodesDownException($"Tried to send {command.GetType().Name} request to all configured nodes in the topology, all of them seem to be down or not responding. I've tried to access the following nodes: " + string.Join(",", _nodeSelector?.Topology.Nodes.Select(x => x.Url) ?? new string[0]), _nodeSelector?.Topology, e);
                    }

                    return;
                }

                command.StatusCode = response.StatusCode;

                var refreshTopology = response.GetBoolHeader(Constants.Headers.RefreshTopology) ?? false;
                var refreshClientConfiguration = response.GetBoolHeader(Constants.Headers.RefreshClientConfiguration) ?? false;

                try
                {
                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        cachedItem.NotModified();

                        if (command.ResponseType == RavenCommandResponseType.Object)
                            command.SetResponse(cachedValue, fromCache: true);

                        return;
                    }

                    if (response.IsSuccessStatusCode == false)
                    {
                        if (await HandleUnsuccessfulResponse(chosenNode, nodeIndex, context, command, request, response, url, sessionId, shouldRetry).ConfigureAwait(false) == false)
                        {   
                            if (response.Headers.TryGetValues("Database-Missing", out var databaseMissing))
                            {
                                var name = databaseMissing.FirstOrDefault();
                                if (name != null)
                                    throw new DatabaseDoesNotExistException(name);
                            }

                            if (command.FailedNodes.Count == 0) //precaution, should never happen at this point
                                throw new InvalidOperationException("Received unsuccessful response and couldn't recover from it. Also, no record of exceptions per failed nodes. This is weird and should not happen.");

                            if (command.FailedNodes.Count == 1)
                            {
                                var node = command.FailedNodes.First();
                                throw node.Value;
                            }

                            throw new AllTopologyNodesDownException("Received unsuccessful response from all servers and couldn't recover from it.",
                                new AggregateException(command.FailedNodes.Select(x => new UnsuccessfulRequestException(x.Key.Url, x.Value))));
                        }
                        return; // we either handled this already in the unsuccessful response or we are throwing
                    }

                    responseDispose = await command.ProcessResponse(context, Cache, response, url).ConfigureAwait(false);
                    _lastReturnedResponse = DateTime.UtcNow;
                }
                finally
                {
                    if (responseDispose == ResponseDisposeHandling.Automatic)
                    {
                        response.Dispose();
                    }
                    if (refreshTopology || refreshClientConfiguration)
                    {
                        var tasks = new Task[2];

                        tasks[0] = refreshTopology
                            ? UpdateTopologyAsync(new ServerNode
                            {
                                Url = chosenNode.Url,
                                Database = _databaseName
                            }, 0)
                            : Task.CompletedTask;

                        tasks[1] = refreshClientConfiguration
                            ? UpdateClientConfigurationAsync()
                            : Task.CompletedTask;

                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    }
                }
            }
        }

        public bool InSpeedTestPhase => _nodeSelector?.InSpeedTestPhase ?? false;

        private bool ShouldExecuteOnAll<TResult>(ServerNode chosenNode, RavenCommand<TResult> command)
        {
            return _readBalanceBehavior == ReadBalanceBehavior.FastestNode &&
                   _nodeSelector != null &&
                   _nodeSelector.InSpeedTestPhase &&
                   _nodeSelector.Topology?.Nodes?.Count > 1 &&
                   command.IsReadRequest &&
                   command.ResponseType == RavenCommandResponseType.Object &&
                   chosenNode != null;
        }

        private static readonly Task<HttpRequestMessage> NeverEndingRequest = new TaskCompletionSource<HttpRequestMessage>().Task;

        private async Task ExecuteOnAllToFigureOutTheFastest<TResult>(ServerNode chosenNode, RavenCommand<TResult> command, Task<HttpResponseMessage> preferredTask,
            CancellationToken token = default(CancellationToken))
        {
            int numberOfFailedTasks = 0;

            var nodes = _nodeSelector.Topology.Nodes;
            var tasks = new Task[nodes.Count];
            for (int i = 0; i < nodes.Count; i++)
            {
                if (nodes[i].ClusterTag == chosenNode.ClusterTag)
                {
                    tasks[i] = preferredTask;
                    continue;
                }

                IDisposable disposable = null;

                try
                {
                    disposable = ContextPool.AllocateOperationContext(out var tmpCtx);
                    var request = CreateRequest(tmpCtx, nodes[i], command, out var _);

                    Interlocked.Increment(ref NumberOfServerRequests);
                    tasks[i] = command.SendAsync(_httpClient, request, token).ContinueWith(x =>
                    {
                        try
                        {
                            if (x.Exception != null)
                            {
                                // we need to make sure that the response is 
                                // properly disposed from all the calls 
                                x.Result.Dispose();
                            }
                        }
                        catch (Exception)
                        {
                            // there is really nothing we can do here
                        }
                        finally 
                        {
                            disposable?.Dispose();
                        }
                    }, token);
                }
                catch (Exception)
                {
                    numberOfFailedTasks++;
                    // nothing we can do about it
                    tasks[i] = NeverEndingRequest;
                    disposable?.Dispose();
                }
            }

            while (numberOfFailedTasks < tasks.Length)
            {
                // here we rely on WhenAny NOT throwing if the completed
                // task has failed
                var completed = await Task.WhenAny(tasks).ConfigureAwait(false);
                var index = Array.IndexOf(tasks, completed);
                if (completed.IsCanceled || completed.IsFaulted)
                {
                    tasks[index] = NeverEndingRequest;
                    numberOfFailedTasks++;
                    continue;
                }
                _nodeSelector.RecordFastest(index, nodes[index]);
                return;
            }
            // we can reach here if the number of failed task equal to the nuber
            // of the nodes, in which case we have nothing to do
        }

        private static void ThrowTimeoutTooLarge(TimeSpan? timeout)
        {
            throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{timeout}'.");
        }

        private HttpCache.ReleaseCacheItem GetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, string url, out string cachedChangeVector, out BlittableJsonReaderObject cachedValue)
        {
            if (command.IsReadRequest && command.ResponseType == RavenCommandResponseType.Object)
            {
                return Cache.Get(context, url, out cachedChangeVector, out cachedValue);
            }

            cachedChangeVector = null;
            cachedValue = null;
            return new HttpCache.ReleaseCacheItem();
        }

        public static readonly string ClientVersion = typeof(RequestExecutor).GetTypeInfo().Assembly.GetName().Version.ToString();

        private HttpRequestMessage CreateRequest<TResult>(JsonOperationContext ctx, ServerNode node, RavenCommand<TResult> command, out string url)
        {
            var request = command.CreateRequest(ctx, node, out url);

            request.RequestUri = new Uri(url);

            if (!request.Headers.Contains("Raven-Client-Version"))
                request.Headers.Add("Raven-Client-Version", ClientVersion);
            return request;
        }

        public event Action<StringBuilder> AdditionalErrorInformation;

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ServerNode chosenNode, int? nodeIndex, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, string url, int? sessionId, bool shouldRetry)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    Cache.SetNotFound(url);
                    if (command.ResponseType == RavenCommandResponseType.Empty)
                        return true;
                    else if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse(null, fromCache: false);
                    else
                        command.SetResponseRaw(response, null, context);
                    return true;
                case HttpStatusCode.Forbidden:
                    throw new AuthorizationException("Forbidden access to " + chosenNode.Database + "@" + chosenNode.Url + ", " +
                        (Certificate == null ? "a certificate is required." : Certificate.FriendlyName + " does not have permission to access it or is unknown.") +
                        request.Method + " " + request.RequestUri);
                case HttpStatusCode.Gone: // request not relevant for the chosen node - the database has been moved to a different one
                    if (shouldRetry == false)
                        return false;

                    await UpdateTopologyAsync(chosenNode, Timeout.Infinite, forceUpdate: true).ConfigureAwait(false);
                    var (index, node) = ChooseNodeForRequest(command, sessionId ?? 0);
                    await ExecuteAsync(node, index, context, command, shouldRetry: false, sessionId: sessionId).ConfigureAwait(false);
                    return true;
                case HttpStatusCode.GatewayTimeout:
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, null).ConfigureAwait(false);
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

        private async Task<bool> HandleServerDown<TResult>(string url, ServerNode chosenNode, int? nodeIndex, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, Exception e)
        {
            if (command.FailedNodes == null)
                command.FailedNodes = new Dictionary<ServerNode, Exception>();

            await AddFailedResponseToCommand(chosenNode, context, command, request, response, e).ConfigureAwait(false);

            if (nodeIndex.HasValue == false)
            {
                //We executed request over a node not in the topology. This means no failover...
                return false;
            }

            SpawnHealthChecks(chosenNode, nodeIndex.Value);

            if (_nodeSelector == null)
                return false;

            _nodeSelector.OnFailedRequest(nodeIndex.Value);

            var (currentIndex, currentNode) = _nodeSelector.GetPreferredNode();
            if (command.FailedNodes.ContainsKey(currentNode))
            {
                return false; //we tried all the nodes...nothing left to do
            }

            OnFailedRequest(url, e);

            await ExecuteAsync(currentNode, currentIndex, context, command).ConfigureAwait(false);

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
                        await PerformHealthCheck(serverNode, nodeStatus.NodeIndex, context).ConfigureAwait(false);
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
                    _nodeSelector?.RestoreNodeIndex(nodeStatus.NodeIndex);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to check node topology, will ignore this node until next topology update", e);
            }
        }

        protected virtual Task PerformHealthCheck(ServerNode serverNode, int nodeIndex, JsonOperationContext context)
        {
            return ExecuteAsync(serverNode, nodeIndex, context, new GetStatisticsCommand(debugTag: "failure=check"), shouldRetry: false);
        }

        private static async Task AddFailedResponseToCommand<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, Exception e)
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
                        command.FailedNodes.Add(chosenNode, ExceptionDispatcher.Get(JsonDeserializationClient.ExceptionSchema(responseJson), response.StatusCode));
                    }
                }
                catch
                {
                    // we failed to parse the error
                    ms.Position = 0;
                    command.FailedNodes.Add(chosenNode, ExceptionDispatcher.Get(new ExceptionDispatcher.ExceptionSchema
                    {
                        Url = request.RequestUri.ToString(),
                        Message = "Got unrecognized response from the server",
                        Error = new StreamReader(ms).ReadToEnd(),
                        Type = "Unparseable Server Response"
                    }, response.StatusCode));
                }
                return;
            }
            //this would be connections that didn't have response, such as "couldn't connect to remote server"
            command.FailedNodes.Add(chosenNode, ExceptionDispatcher.Get(new ExceptionDispatcher.ExceptionSchema
            {
                Url = request.RequestUri.ToString(),
                Message = e.Message,
                Error = e.ToString(),
                Type = e.GetType().FullName
            }, HttpStatusCode.InternalServerError));
        }

        protected bool _disposed;
        protected Task _firstTopologyUpdate;
        protected string[] _lastKnownUrls;
        private readonly HttpClient _httpClient;

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
            _updateTopologyTimer?.Dispose();
            DisposeAllFailedNodesTimers();
            // shared instance, cannot dispose!
            //_httpClient.Dispose();
        }

        private HttpClient CreateClient()
        {
            var httpMessageHandler = new HttpClientHandler();
            if (PlatformDetails.RunningOnMacOsx)
            {
                var callback = ServerCertificateCustomValidationCallback;
                if (callback != null)
                    throw new PlatformNotSupportedException("On Mac OSX, is it not possible to register to ServerCertificateCustomValidationCallback because of https://github.com/dotnet/corefx/issues/9728");
            }
            else
            {
                httpMessageHandler.ServerCertificateCustomValidationCallback += OnServerCertificateCustomValidationCallback;
            }
            if (Certificate != null)
            {
                httpMessageHandler.ClientCertificates.Add(Certificate);
                httpMessageHandler.SslProtocols = SslProtocols.Tls12;

                ValidateClientKeyUsages(Certificate);
            }

            return new HttpClient(httpMessageHandler)
            {
                Timeout = GlobalHttpClientTimeout
            };
        }

        private static void ValidateClientKeyUsages(X509Certificate2 certificate)
        {
            var supported = false;
            foreach (var extension in certificate.Extensions)
            {
                if (extension.Oid.Value != "2.5.29.37") //Enhanced Key Usage extension
                    continue;

                var extensionsString = new AsnEncodedData(extension.Oid, extension.RawData).Format(false);

                supported = extensionsString.Contains("1.3.6.1.5.5.7.3.2"); // Client Authentication
            }

            if (supported == false)
                throw new InvalidOperationException("Client certificate " + certificate.FriendlyName + " must be defined with the following 'Enhanced Key Usage': Client Authentication (Oid 1.3.6.1.5.5.7.3.2)");
        }

        public static event Func<HttpRequestMessage, X509Certificate2, X509Chain, SslPolicyErrors, bool> ServerCertificateCustomValidationCallback;

        private static bool OnServerCertificateCustomValidationCallback(HttpRequestMessage msg, X509Certificate2 cert, X509Chain chain, SslPolicyErrors errors)
        {
            var onServerCertificateCustomValidationCallback = ServerCertificateCustomValidationCallback;
            if (onServerCertificateCustomValidationCallback == null)
                return errors == SslPolicyErrors.None;
            return onServerCertificateCustomValidationCallback(msg, cert, chain, errors);
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

        public async Task<(int, ServerNode)> GetPreferredNode()
        {
            await EnsureNodeSelector().ConfigureAwait(false);

            return _nodeSelector.GetPreferredNode();
        }

        public async Task<(int Index, ServerNode Node)> GetNodeBySessionId(int sessionId)
        {
            await EnsureNodeSelector().ConfigureAwait(false);

            return _nodeSelector.GetNodeBySessionId(sessionId);
        }

        public async Task<(int Index, ServerNode Node)> GetFastestNode()
        {
            await EnsureNodeSelector().ConfigureAwait(false);

            return _nodeSelector.GetFastestNode();
        }

        private async Task EnsureNodeSelector()
        {
            if (_firstTopologyUpdate.Status != TaskStatus.RanToCompletion)
                await _firstTopologyUpdate.ConfigureAwait(false);

            if (_nodeSelector == null)
            {
                _nodeSelector = new NodeSelector(new Topology
                {
                    Nodes = TopologyNodes.ToList(),
                    Etag = TopologyEtag
                });
            }
        }

        protected void OnTopologyUpdated(Topology newTopology)
        {
            TopologyUpdated?.Invoke(newTopology);
        }
    }
}
