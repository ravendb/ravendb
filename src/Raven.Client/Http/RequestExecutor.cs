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
using System.Runtime.ExceptionServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Commands;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Threading;

namespace Raven.Client.Http
{
    public class RequestExecutor : IDisposable
    {
        // https://aspnetmonsters.com/2016/08/2016-08-27-httpclientwrong/

        internal static readonly TimeSpan GlobalHttpClientTimeout = TimeSpan.FromHours(12);

        private static readonly ConcurrentDictionary<string, Lazy<HttpClient>> GlobalHttpClientWithCompression = new ConcurrentDictionary<string, Lazy<HttpClient>>();
        private static readonly ConcurrentDictionary<string, Lazy<HttpClient>> GlobalHttpClientWithoutCompression = new ConcurrentDictionary<string, Lazy<HttpClient>>();

        private static readonly GetStatisticsOperation FailureCheckOperation = new GetStatisticsOperation(debugTag: "failure=check");

        private readonly SemaphoreSlim _updateDatabaseTopologySemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _updateClientConfigurationSemaphore = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<ServerNode, NodeStatus> _failedNodesTimers = new ConcurrentDictionary<ServerNode, NodeStatus>();

        public X509Certificate2 Certificate { get; }
        private readonly string _databaseName;

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecutor>("Client");
        private DateTime _lastReturnedResponse;

        protected readonly ReadBalanceBehavior _readBalanceBehavior;

        public readonly JsonContextPool ContextPool;

        public readonly AsyncLocal<AggressiveCacheOptions> AggressiveCaching = new AsyncLocal<AggressiveCacheOptions>();

        public readonly HttpCache Cache;

        public Topology Topology => _nodeSelector?.Topology;

        private ServerNode _topologyTakenFromNode;

        public HttpClient HttpClient { get; private set; }

        public IReadOnlyList<ServerNode> TopologyNodes => _nodeSelector?.Topology.Nodes;

        private Timer _updateTopologyTimer;

        protected NodeSelector _nodeSelector;

        private TimeSpan? _defaultTimeout;

        public long NumberOfServerRequests;

        protected readonly string TopologyHash;

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

        private HttpClient GetCachedOrCreateHttpClient(ConcurrentDictionary<string, Lazy<HttpClient>> httpClientCache) =>
            httpClientCache.GetOrAdd(Certificate?.Thumbprint ?? string.Empty, new Lazy<HttpClient>(CreateClient)).Value;

        protected RequestExecutor(string databaseName, X509Certificate2 certificate, DocumentConventions conventions, string[] initialUrls)
        {
            Cache = new HttpCache(conventions.MaxHttpCacheSize.GetValue(SizeUnit.Bytes));

            _disposeOnceRunner = new DisposeOnce<ExceptionRetry>(() =>
            {
                Cache.Dispose();
                ContextPool.Dispose();
                _updateTopologyTimer?.Dispose();
                DisposeAllFailedNodesTimers();
                // shared instance, cannot dispose!
                //_httpClient.Dispose();
            });

            _readBalanceBehavior = conventions.ReadBalanceBehavior;
            _databaseName = databaseName;
            Certificate = certificate;

            _lastReturnedResponse = DateTime.UtcNow;

            ContextPool = new JsonContextPool();
            Conventions = conventions.Clone();
            DefaultTimeout = Conventions.RequestTimeout;

            var thumbprint = string.Empty;
            if (certificate != null)
                thumbprint = certificate.Thumbprint;

            var httpClientCache = conventions.UseCompression ?
                GlobalHttpClientWithCompression :
                GlobalHttpClientWithoutCompression;

           

            _serverCallbackRWLock.EnterReadLock();
            try
            {
                _updateHttpHandlerDelegate = UpdateHttpClient;
                _liveClients.TryAdd(new WeakReference<Action>(_updateHttpHandlerDelegate), null);
                _updateHttpHandlerDelegate();
            }
            finally
            {
                _serverCallbackRWLock.ExitReadLock();
            }


            TopologyHash = Http.TopologyHash.GetTopologyHash(initialUrls);

            void UpdateHttpClient()
            {
                HttpClient = httpClientCache.TryGetValue(thumbprint, out var lazyClient) == false ?
                                GetCachedOrCreateHttpClient(httpClientCache) : lazyClient.Value;
            }
        }

      

        public static RequestExecutor Create(string[] initialUrls, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            var executor = new RequestExecutor(databaseName, certificate, conventions, initialUrls);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(initialUrls);
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
            var initialUrls = new[] { url };
            url = ValidateUrls(initialUrls, certificate)[0];
            var executor = new RequestExecutor(databaseName, certificate, conventions, initialUrls)
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
            if (Disposed)
                return;

            await _updateClientConfigurationSemaphore.WaitAsync().ConfigureAwait(false);

            var oldDisableClientConfigurationUpdates = _disableClientConfigurationUpdates;
            _disableClientConfigurationUpdates = true;

            try
            {
                if (Disposed)
                    return;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetClientConfigurationOperation.GetClientConfigurationCommand();

                    var (currentIndex, currentNode) = ChooseNodeForRequest(command);
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
            if (Disposed)
                return false;

            //prevent double topology updates if execution takes too much time
            // --> in cases with transient issues
            var lockTaken = await _updateDatabaseTopologySemaphore.WaitAsync(timeout).ConfigureAwait(false);
            if (lockTaken == false)
                return false;

            try
            {
                if (Disposed)
                    return false;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetDatabaseTopologyCommand();
                    await ExecuteAsync(node, null, context, command, shouldRetry: false).ConfigureAwait(false);
                    var topology = command.Result;

                    DatabaseTopologyLocalCache.TrySaving(_databaseName, TopologyHash, topology, context);

                    if (_nodeSelector == null)
                    {
                        _nodeSelector = new NodeSelector(topology);

                        if (_readBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }
                    else if (_nodeSelector.OnUpdateTopology(topology, forceUpdate: forceUpdate))
                    {
                        DisposeAllFailedNodesTimers();
                        if (_readBalanceBehavior == ReadBalanceBehavior.FastestNode)
                        {
                            _nodeSelector.ScheduleSpeedTest();
                        }
                    }

                    TopologyEtag = _nodeSelector.Topology.Etag;
                    OnTopologyUpdated(topology);
                }
            }
            finally
            {
                _updateDatabaseTopologySemaphore.Release();
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

        public void Execute<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            SessionInfo sessionInfo = null)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context, sessionInfo));
        }

        public Task ExecuteAsync<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            SessionInfo sessionInfo = null,
            CancellationToken token = default)
        {
            var topologyUpdate = _firstTopologyUpdate;

            if (topologyUpdate != null && topologyUpdate.Status == TaskStatus.RanToCompletion || _disableTopologyUpdates)
            {
                var (nodeIndex, chosenNode) = ChooseNodeForRequest(command, sessionInfo);
                return ExecuteAsync(chosenNode, nodeIndex, context, command, shouldRetry: true, sessionInfo: sessionInfo, token: token);
            }

            return UnlikelyExecuteAsync(command, context, topologyUpdate, sessionInfo, token);
        }

        public (int CurrentIndex, ServerNode CurrentNode) ChooseNodeForRequest<TResult>(RavenCommand<TResult> cmd, SessionInfo sessionInfo = null)
        {
            if (cmd.IsReadRequest == false)
            {
                return _nodeSelector.GetPreferredNode();
            }

            switch (_readBalanceBehavior)
            {
                case ReadBalanceBehavior.None:
                    return _nodeSelector.GetPreferredNode();
                case ReadBalanceBehavior.RoundRobin:
                    return _nodeSelector.GetNodeBySessionId(sessionInfo?.SessionId ?? 0);
                case ReadBalanceBehavior.FastestNode:
                    return _nodeSelector.GetFastestNode();
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task UnlikelyExecuteAsync<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            Task topologyUpdate,
            SessionInfo sessionInfo,
            CancellationToken token)
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

            var (currentIndex, currentNode) = ChooseNodeForRequest(command, sessionInfo);
            await ExecuteAsync(currentNode, currentIndex, context, command, true, sessionInfo, token).ConfigureAwait(false);
        }

        private void UpdateTopologyCallback(object _)
        {
            var time = DateTime.UtcNow;
            if (time - _lastReturnedResponse <= TimeSpan.FromMinutes(5))
                return;

            ServerNode serverNode;

            try
            {
                var preferredNode = _nodeSelector.GetPreferredNode();
                serverNode = preferredNode.Node;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Couldn't get preferred node Topology from _updateTopologyTimer task", e);
                return;
            }
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
            initialUrls = ValidateUrls(initialUrls, Certificate);

            var list = new List<(string, Exception)>();
            foreach (var url in initialUrls)
            {
                try
                {
                    var serverNode = new ServerNode
                    {
                        Url = url,
                        Database = _databaseName
                    };

                    await UpdateTopologyAsync(serverNode, Timeout.Infinite).ConfigureAwait(false);

                    InitializeUpdateTopologyTimer();
                    _topologyTakenFromNode = serverNode;
                    return;
                }
                catch (AuthorizationException)
                {
                    // auth exceptions will always happen, on all nodes
                    // so errors immediately
                    _lastKnownUrls = initialUrls;
                    throw;
                }
                catch (DatabaseDoesNotExistException)
                {
                    // Will happen on all node in the cluster,
                    // so errors immediately
                    _lastKnownUrls = initialUrls;
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
                if (TryLoadFromCache(context))
                {
                    InitializeUpdateTopologyTimer();
                    return;
                }
            }

            _lastKnownUrls = initialUrls;

            ThrowExceptions(list);
        }

        protected virtual void ThrowExceptions(List<(string, Exception)> list)
        {
            var message = "Failed to retrieve database topology from all known nodes.";
            foreach (var tuple in list)
            {
                message += Environment.NewLine;
                message += tuple.Item1;
                message += " -> ";
                var ex = tuple.Item2;
                if (ex == null)
                    message += " No exception.";
                else
                {
                    if (ex is AggregateException aggregateException)
                        ex = aggregateException.ExtractSingleInnerException();

                    message += ex.AllInnerMessages();
                }
            }

            throw new AggregateException(message, list.Select(x => x.Item2));
        }

        protected static string[] ValidateUrls(string[] initialUrls, X509Certificate2 certificate)
        {
            var cleanUrls = new string[initialUrls.Length];
            var requireHttps = certificate != null;
            for (var index = 0; index < initialUrls.Length; index++)
            {
                var url = initialUrls[index];
                if (Uri.TryCreate(url, UriKind.Absolute, out var uri) == false)
                    throw new InvalidOperationException("The url '" + url + "' is not valid");
                cleanUrls[index] = uri.ToString().TrimEnd('/', ' ');
                requireHttps |= string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase);
            }

            if (requireHttps == false)
                return cleanUrls;

            foreach (var url in initialUrls)
            {
                var uri = new Uri(url); // verified it works above

                if (string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (certificate != null)
                    throw new InvalidOperationException("The url " + url + " is using HTTP, but a certificate is specified, which require us to use HTTPS");

                throw new InvalidOperationException("The url " + url + " is using HTTP, but other urls are using HTTPS, and mixing of HTTP and HTTPS is not allowed");
            }
            return cleanUrls;
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

        protected virtual bool TryLoadFromCache(JsonOperationContext context)
        {
            var cachedTopology = DatabaseTopologyLocalCache.TryLoad(_databaseName, TopologyHash, context);
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
            bool shouldRetry = true,
            SessionInfo sessionInfo = null,
            CancellationToken token = default)
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
                        command.CanCacheAggressively)
                    {
                        if ((cachedItem.Item.Flags & HttpCache.ItemFlags.NotFound) != HttpCache.ItemFlags.None)
                        {
                            // if this is a cached delete, we only respect it if it _came_ from an aggresively cached
                            // block, otherwise, we'll run the request again
                            if ((cachedItem.Item.Flags & HttpCache.ItemFlags.AggresivelyCached) == HttpCache.ItemFlags.AggresivelyCached)
                            {
                                command.SetResponse(context, cachedValue, fromCache: true);
                                return;
                            }
                        }
                        else
                        {
                            command.SetResponse(context, cachedValue, fromCache: true);
                            return;
                        }

                    }

                    request.Headers.TryAddWithoutValidation("If-None-Match", $"\"{cachedChangeVector}\"");
                }

                if (sessionInfo?.AsyncCommandRunning ?? false)
                    ThrowInvalidConcurrentSessionUsage(command.GetType().Name, sessionInfo);

                if (_disableClientConfigurationUpdates == false)
                    request.Headers.TryAddWithoutValidation(Constants.Headers.ClientConfigurationEtag, $"\"{ClientConfigurationEtag.ToInvariantString()}\"");

                if (_disableTopologyUpdates == false)
                    request.Headers.TryAddWithoutValidation(Constants.Headers.TopologyEtag, $"\"{TopologyEtag.ToInvariantString()}\"");

                var sp = Stopwatch.StartNew();
                HttpResponseMessage response = null;
                var responseDispose = ResponseDisposeHandling.Automatic;
                try
                {
                    if (sessionInfo != null)
                    {
                        sessionInfo.AsyncCommandRunning = true;
                    }

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
                                var preferredTask = command.SendAsync(HttpClient, request, cts.Token);
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

                                    if (sessionInfo != null)
                                        sessionInfo.AsyncCommandRunning = false;

                                    if (await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, timeoutException, sessionInfo).ConfigureAwait(false) == false)
                                        ThrowFailedToContactAllNodes(command, request);

                                    return;
                                }

                                throw;
                            }
                        }
                    }
                    else
                    {
                        var preferredTask = command.SendAsync(HttpClient, request, token);
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

                    if (sessionInfo != null)
                        sessionInfo.AsyncCommandRunning = false;


                    if (await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, e, sessionInfo).ConfigureAwait(false) == false)
                    {
                        ThrowIfClientException(response, e);
                        ThrowFailedToContactAllNodes(command, request);
                    }

                    return;
                }
                finally
                {
                    if (sessionInfo != null)
                        sessionInfo.AsyncCommandRunning = false;
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
                            command.SetResponse(context, cachedValue, fromCache: true);

                        return;
                    }

                    if (response.IsSuccessStatusCode == false)
                    {
                        if (await HandleUnsuccessfulResponse(chosenNode, nodeIndex, context, command, request, response, url, sessionInfo, shouldRetry).ConfigureAwait(false) == false)
                        {
                            if (response.Headers.TryGetValues("Database-Missing", out var databaseMissing))
                            {
                                var name = databaseMissing.FirstOrDefault();
                                if (name != null)
                                    DatabaseDoesNotExistException.Throw(name);
                            }

                            ThrowFailedToContactAllNodes(command, request);
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

        private void ThrowFailedToContactAllNodes<TResult>(RavenCommand<TResult> command, HttpRequestMessage request)
        {
            if (command.FailedNodes.Count == 0) //precaution, should never happen at this point
                throw new InvalidOperationException("Received unsuccessful response and couldn't recover from it. Also, no record of exceptions per failed nodes. " +
                                                    "This is weird and should not happen.");

            if (command.FailedNodes.Count == 1)
                ExceptionDispatchInfo.Capture(command.FailedNodes.First().Value).Throw();

            var message = $"Tried to send '{command.GetType().Name}' request via `{request.Method} {request.RequestUri.PathAndQuery}` " +
                          $"to all configured nodes in the topology, none of the attempt succeeded. {Environment.NewLine}";

            if (_topologyTakenFromNode != null)
                message += $"I was able to fetch {_topologyTakenFromNode.Database} topology from {_topologyTakenFromNode.Url}.{Environment.NewLine}";

            var nodes = _nodeSelector?.Topology.Nodes;
            if (nodes == null)
            {
                message += "Topology is empty.";
            }
            else
            {
                message += "Topology:";
                foreach (var node in nodes)
                {
                    command.FailedNodes.TryGetValue(node, out var exception);
                    message += Environment.NewLine +
                               $"[Url: {node.Url}, " +
                               $"ClusterTag: {node.ClusterTag}, " +
                               $"ServerRole: {node.ServerRole}, " +
                               $"Exception: {exception?.AllInnerMessages() ?? "No exception"}]";
                }
            }

            throw new AllTopologyNodesDownException(message, _nodeSelector?.Topology,
                new AggregateException(command.FailedNodes.Select(x => new UnsuccessfulRequestException(x.Key.Url, x.Value))));
        }

        private static void ThrowInvalidConcurrentSessionUsage(string command, SessionInfo sessionInfo)
        {
            throw new InvalidOperationException($"Cannot execute async command {command} while another async command is running in the same session {sessionInfo.SessionId}");
        }

        public bool InSpeedTestPhase => _nodeSelector?.InSpeedTestPhase ?? false;

        private bool ShouldExecuteOnAll<TResult>(ServerNode chosenNode, RavenCommand<TResult> command)
        {
            if (_readBalanceBehavior != ReadBalanceBehavior.FastestNode)
                return false;

            var selector = _nodeSelector;

            return selector != null &&
                   selector.InSpeedTestPhase &&
                   selector.Topology?.Nodes?.Count > 1 &&
                   command.IsReadRequest &&
                   command.ResponseType == RavenCommandResponseType.Object &&
                   chosenNode != null;
        }

        private static readonly Task<HttpRequestMessage> NeverEndingRequest = new TaskCompletionSource<HttpRequestMessage>(TaskCreationOptions.RunContinuationsAsynchronously).Task;

        private async Task ExecuteOnAllToFigureOutTheFastest<TResult>(ServerNode chosenNode, RavenCommand<TResult> command, Task<HttpResponseMessage> preferredTask,
            CancellationToken token = default)
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
                    tasks[i] = command.SendAsync(HttpClient, request, token).ContinueWith(x =>
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
            if (command.CanCache && command.IsReadRequest && command.ResponseType == RavenCommandResponseType.Object)
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

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ServerNode chosenNode, int? nodeIndex, JsonOperationContext context, RavenCommand<TResult> command, HttpRequestMessage request, HttpResponseMessage response, string url, SessionInfo sessionInfo, bool shouldRetry)
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    Cache.SetNotFound(url, AggressiveCaching.Value != null);
                    if (command.ResponseType == RavenCommandResponseType.Empty)
                        return true;
                    else if (command.ResponseType == RavenCommandResponseType.Object)
                        command.SetResponse(context, null, fromCache: false);
                    else
                        command.SetResponseRaw(response, null, context);
                    return true;
                case HttpStatusCode.Forbidden:
                    throw new AuthorizationException("Forbidden access to " + chosenNode.Database + "@" + chosenNode.Url + ", " +
                        (Certificate == null ? "a certificate is required." : Certificate.FriendlyName + " does not have permission to access it or is unknown. ") +
                        $"Method: {request.Method}, Request: {request.RequestUri}");
                case HttpStatusCode.Gone: // request not relevant for the chosen node - the database has been moved to a different one
                    if (shouldRetry == false)
                        return false;

                    await UpdateTopologyAsync(chosenNode, Timeout.Infinite, forceUpdate: true).ConfigureAwait(false);
                    var (index, node) = ChooseNodeForRequest(command, sessionInfo);
                    await ExecuteAsync(node, index, context, command, shouldRetry: false, sessionInfo: sessionInfo).ConfigureAwait(false);
                    return true;
                case HttpStatusCode.GatewayTimeout:
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    return await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, null, sessionInfo).ConfigureAwait(false);
                case HttpStatusCode.Conflict:
                    await HandleConflict(context, response).ConfigureAwait(false);
                    break;
                default:
                    command.OnResponseFailure(response);
                    await ExceptionDispatcher.Throw(context, response, AdditionalErrorInformation).ConfigureAwait(false);
                    break;
            }
            return false;
        }

        private static Task HandleConflict(JsonOperationContext context, HttpResponseMessage response)
        {
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

        private async Task<bool> HandleServerDown<TResult>(string url, ServerNode chosenNode, int? nodeIndex, JsonOperationContext context, RavenCommand<TResult> command,
            HttpRequestMessage request, HttpResponseMessage response, Exception e, SessionInfo sessionInfo)
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

            await ExecuteAsync(currentNode, currentIndex, context, command, sessionInfo: sessionInfo).ConfigureAwait(false);

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
            return ExecuteAsync(serverNode, nodeIndex, context, FailureCheckOperation.GetCommand(Conventions, context), shouldRetry: false);
        }

        private static async Task AddFailedResponseToCommand<TResult>(ServerNode chosenNode, JsonOperationContext context, RavenCommand<TResult> command,
            HttpRequestMessage request, HttpResponseMessage response, Exception e)
        {
            if (response != null)
            {
                var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
                var ms = new MemoryStream(); // todo: have a pool of those
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

        protected Task _firstTopologyUpdate;
        protected string[] _lastKnownUrls;
        private readonly DisposeOnce<ExceptionRetry> _disposeOnceRunner;
        protected bool Disposed => _disposeOnceRunner.Disposed;

        public virtual void Dispose()
        {
            if (_disposeOnceRunner.Disposed)
                return;

            _updateDatabaseTopologySemaphore.Wait();
            _disposeOnceRunner.Dispose();
        }

        public static HttpClientHandler CreateHttpMessageHandler(X509Certificate2 certificate, bool setSslProtocols, bool useCompression, bool hasExplicitlySetCompressionUsage = false)
        {
            var httpMessageHandler = new HttpClientHandler();
            if (httpMessageHandler.SupportsAutomaticDecompression)
            {
                httpMessageHandler.AutomaticDecompression =
                    useCompression ?
                        DecompressionMethods.GZip | DecompressionMethods.Deflate
                        : DecompressionMethods.None;
            }
            else if (httpMessageHandler.SupportsAutomaticDecompression == false &&
                     useCompression &&
                     hasExplicitlySetCompressionUsage)
            {
                throw new NotSupportedException("HttpClient implementation for the current platform does not support request compression.");
            }

            var serverCallBack = _serverCertificateCustomValidationCallback;
            if (serverCallBack != null)
            {
                if (PlatformDetails.RunningOnMacOsx)
                {
                    throw new PlatformNotSupportedException("On Mac OSX, is it not possible to register to ServerCertificateCustomValidationCallback because of https://github.com/dotnet/corefx/issues/9728");
                }
                else
                {
                    httpMessageHandler.ServerCertificateCustomValidationCallback += OnServerCertificateCustomValidationCallback;
                }
            }

            if (certificate != null)
            {
                httpMessageHandler.ClientCertificates.Add(certificate);
                try
                {
                    if (setSslProtocols)
                        httpMessageHandler.SslProtocols = SslProtocols.Tls12;
                }
                catch (PlatformNotSupportedException)
                {
                    // The user can set the following manually:
                    // ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                }

                ValidateClientKeyUsages(certificate);
            }

            return httpMessageHandler;
        }

        public HttpClient CreateClient()
        {
            var httpMessageHandler = CreateHttpMessageHandler(Certificate,
                setSslProtocols: true,
                useCompression: Conventions.UseCompression,
                hasExplicitlySetCompressionUsage: Conventions.HasExplicitlySetCompressionUsage);
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

                if (!(extension is X509EnhancedKeyUsageExtension kue))
                    continue;

                foreach (var eku in kue.EnhancedKeyUsages)
                {
                    if (eku.Value != "1.3.6.1.5.5.7.3.2")
                        continue;

                    supported = true;
                    break;
                }

                if (supported)
                    break;
            }

            if (supported == false)
                throw new InvalidOperationException("Client certificate " + certificate.FriendlyName + " must be defined with the following 'Enhanced Key Usage': Client Authentication (Oid 1.3.6.1.5.5.7.3.2)");
        }

        private static Func<HttpRequestMessage, X509Certificate2, X509Chain, SslPolicyErrors, bool> _serverCertificateCustomValidationCallback;
        private static ConcurrentDictionary<WeakReference<Action>, object> _liveClients = new ConcurrentDictionary<WeakReference<Action>, object>();
        private Action _updateHttpHandlerDelegate;// we need this to hold a reference to the action as long as the executer is alive
        private static readonly ReaderWriterLockSlim _serverCallbackRWLock = new ReaderWriterLockSlim();

        public static event Func<HttpRequestMessage, X509Certificate2, X509Chain, SslPolicyErrors, bool> ServerCertificateCustomValidationCallback
        {
            add
            {
                _serverCallbackRWLock.EnterWriteLock();
                try
                {
                    _serverCertificateCustomValidationCallback += value;
                    ForceUpdateOfAllHttpClients();
                }
                finally
                {
                    _serverCallbackRWLock.ExitWriteLock();
                }
            }
            remove
            {
                _serverCertificateCustomValidationCallback -= value;
                ForceUpdateOfAllHttpClients();
            }
        }

        private static void ForceUpdateOfAllHttpClients()
        {
            // change for the server callback requires changing http handlers
            GlobalHttpClientWithCompression.Clear();
            GlobalHttpClientWithoutCompression.Clear();
            foreach (var client in _liveClients.Keys)
            {
                if (client.TryGetTarget(out var updateHttpClient))
                {
                    try
                    {
                        updateHttpClient();
                    }
                    catch (Exception)
                    {
                        // we can't really handle this, so we won't
                        // try, this should be very odd behavior
                    }
                }
                else
                    _liveClients.TryRemove(client, out _);
            }
        }

        private static bool OnServerCertificateCustomValidationCallback(HttpRequestMessage msg, X509Certificate2 cert, X509Chain chain, SslPolicyErrors errors)
        {
            var onServerCertificateCustomValidationCallback = _serverCertificateCustomValidationCallback;
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
                if (_requestExecutor.Disposed)
                {
                    Dispose();
                    return;
                }
                GC.KeepAlive(_requestExecutor.CheckNodeStatusCallback(this));
            }

            public void UpdateTimer()
            {
                Debug.Assert(_timer != null);
                _timer?.Change(NextTimerPeriod(), Timeout.InfiniteTimeSpan);
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
            if (_firstTopologyUpdate != null && _firstTopologyUpdate.Status != TaskStatus.RanToCompletion)
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

        private static void ThrowIfClientException(HttpResponseMessage response, Exception e)
        {
            switch (e.InnerException)
            {
                case NullReferenceException _:
                case ObjectDisposedException _:
                case InvalidOperationException _:
                case ArgumentException _:
                case IndexOutOfRangeException _:
                    ExceptionDispatchInfo.Capture(e.InnerException).Throw();
                    break;
                default:
                    return;
            }
        }
    }
}
