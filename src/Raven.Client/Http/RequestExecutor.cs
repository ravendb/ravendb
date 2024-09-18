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
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Routing;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Properties;
using Raven.Client.ServerWide.Commands;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Threading;

namespace Raven.Client.Http
{
    public class RequestExecutor : IDisposable
    {
        private const int DefaultConnectionLimit = int.MaxValue;

        private static readonly Guid GlobalApplicationIdentifier = Guid.NewGuid();

        private static readonly TimeSpan MinHttpClientLifetime = TimeSpan.FromSeconds(5);

        private const int InitialTopologyEtag = -2;

        // https://aspnetmonsters.com/2016/08/2016-08-27-httpclientwrong/

        internal static readonly TimeSpan GlobalHttpClientTimeout = TimeSpan.FromHours(12);

        private static readonly ConcurrentDictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>> GlobalHttpClientCache = new ConcurrentDictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>>();

        private static readonly GetStatisticsOperation BackwardCompatibilityFailureCheckOperation = new GetStatisticsOperation(debugTag: "failure=check");
        private static readonly DatabaseHealthCheckOperation FailureCheckOperation = new DatabaseHealthCheckOperation();
        private static ConcurrentSet<string> _useOldFailureCheckOperation;

        private readonly SemaphoreSlim _updateDatabaseTopologySemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _updateClientConfigurationSemaphore = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<ServerNode, Lazy<NodeStatus>> _failedNodesTimers = new ConcurrentDictionary<ServerNode, Lazy<NodeStatus>>();

        private readonly AsyncLocal<RequestContext> _requestContexts = new AsyncLocal<RequestContext>();

        private readonly ReturnContext _doNotClearContext;
        private readonly ReturnContext _clearContext;

        public X509Certificate2 Certificate { get; }
        private readonly string _databaseName;

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RequestExecutor>("Client");
        
        public readonly JsonContextPool ContextPool;

        public readonly AsyncLocal<AggressiveCacheOptions> AggressiveCaching = new AsyncLocal<AggressiveCacheOptions>();

        public readonly HttpCache Cache;

        public Topology Topology => _nodeSelector?.Topology;

        private ServerNode _topologyTakenFromNode;

        internal string LastServerVersion { get; private set; }

        private HttpClient _httpClient;

        public HttpClient HttpClient
        {
            get
            {
                var httpClient = _httpClient;
                if (httpClient != null)
                    return httpClient;

                return _httpClient = GetHttpClient();
            }
        }

        public IReadOnlyList<ServerNode> TopologyNodes => _nodeSelector?.Topology.Nodes;

        private WeakReferencingTimer _updateTopologyTimer;

        protected internal NodeSelector _nodeSelector;

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

        protected string _topologyHeaderName = Constants.Headers.TopologyEtag;

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

        private TimeSpan _secondBroadcastAttemptTimeout;

        public TimeSpan SecondBroadcastAttemptTimeout
        {
            get => _secondBroadcastAttemptTimeout;
            set
            {
                if (value > GlobalHttpClientTimeout)
                    throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{value}'.");

                _secondBroadcastAttemptTimeout = value;
            }
        }

        private TimeSpan _firstBroadcastAttemptTimeout;

        public TimeSpan FirstBroadcastAttemptTimeout
        {
            get => _firstBroadcastAttemptTimeout;
            set
            {
                if (value > GlobalHttpClientTimeout)
                    throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{value}'.");

                _firstBroadcastAttemptTimeout = value;
            }
        }

        public event EventHandler<(long RaftCommandIndex, ClientConfiguration Configuration)> ClientConfigurationChanged;

        private event EventHandler<FailedRequestEventArgs> _onFailedRequest;

        public event EventHandler<FailedRequestEventArgs> OnFailedRequest
        {
            add
            {
                lock (_locker)
                {
                    _onFailedRequest += value;
                }
            }

            remove
            {
                lock (_locker)
                {
                    _onFailedRequest -= value;
                }
            }
        }

        public event EventHandler<BeforeRequestEventArgs> OnBeforeRequest;

        public event EventHandler<SucceedRequestEventArgs> OnSucceedRequest;

        private void OnFailedRequestInvoke(string url, Exception e, HttpResponseMessage response = null, HttpRequestMessage request = null)
        {
            _onFailedRequest?.Invoke(this, new FailedRequestEventArgs(_databaseName, url, e, response, request));
        }

        private event EventHandler<TopologyUpdatedEventArgs> _onTopologyUpdated;

        public event EventHandler<TopologyUpdatedEventArgs> OnTopologyUpdated
        {
            add
            {
                _onTopologyUpdated += value;
            }

            remove
            {
                _onTopologyUpdated -= value;
            }
        }

        internal void OnTopologyUpdatedInvoke(Topology newTopology)
        {
            _onTopologyUpdated?.Invoke(this, new TopologyUpdatedEventArgs(newTopology));
        }

        private HttpClient GetHttpClient()
        {
            var cacheKey = GetHttpClientCacheKey();

            return GlobalHttpClientCache.GetOrAdd(cacheKey, new Lazy<HttpClientCacheItem>(() => new HttpClientCacheItem
            {
                HttpClient = CreateClient(),
                CreatedAt = SystemTime.UtcNow
            })).Value.HttpClient;
        }

        internal bool TryRemoveHttpClient(bool force = false)
        {
            var cacheKey = GetHttpClientCacheKey();

            if (GlobalHttpClientCache.TryGetValue(cacheKey, out var client) &&
                ((client.IsValueCreated &&
                SystemTime.UtcNow - client.Value.CreatedAt > MinHttpClientLifetime) || force))
            {
                GlobalHttpClientCache.TryRemove(cacheKey, out _);

                _httpClient = null;

                return true;
            }

            return false;
        }

        private HttpClientCacheKey GetHttpClientCacheKey()
        {
#if NETCOREAPP3_1_OR_GREATER
            var httpPooledConnectionLifetime = Conventions.HttpPooledConnectionLifetime;
            var httpPooledConnectionIdleTimeout = Conventions.HttpPooledConnectionIdleTimeout;
#else
            TimeSpan? httpPooledConnectionLifetime = null;
            TimeSpan? httpPooledConnectionIdleTimeout = null;
#endif

            return new HttpClientCacheKey(Certificate?.Thumbprint ?? string.Empty, Conventions.UseCompression, httpPooledConnectionLifetime, httpPooledConnectionIdleTimeout, Conventions.HttpClientType);
        }

        internal static void ClearHttpClientsPool()
        {
            GlobalHttpClientCache.Clear();
        }

        private static bool ShouldRemoveHttpClient(SocketException exception)
        {
            switch (exception.SocketErrorCode)
            {
                case SocketError.HostDown:
                case SocketError.HostNotFound:
                case SocketError.HostUnreachable:
                case SocketError.ConnectionRefused:
                case SocketError.TryAgain:
                    return true;

                default:
                    return false;
            }
        }

        private static readonly Exception ServerCertificateCustomValidationCallbackRegistrationException;

        static RequestExecutor()
        {
            try
            {
                using (var handler = new HttpClientHandler())
                    handler.ServerCertificateCustomValidationCallback += OnServerCertificateCustomValidationCallback;
            }
            catch (Exception e)
            {
                ServerCertificateCustomValidationCallbackRegistrationException = e;
            }
        }

        protected RequestExecutor(string databaseName, X509Certificate2 certificate, DocumentConventions conventions, string[] initialUrls)
        {
            _clearContext = new ReturnContext(_requestContexts, dispose: false);
            _doNotClearContext = new ReturnContext(_requestContexts, dispose: true);

            Cache = new HttpCache(conventions.MaxHttpCacheSize.GetValue(SizeUnit.Bytes));

            _disposeOnceRunner = new DisposeOnce<ExceptionRetry>(() =>
            {
                GC.SuppressFinalize(this);
                Cache.Dispose();
                ContextPool.Dispose();
                _updateTopologyTimer?.Dispose();
                _nodeSelector?.Dispose();
                DisposeAllFailedNodesTimers();
                // shared instance, cannot dispose!
                //_httpClient.Dispose();
            });

            _databaseName = databaseName;
            Certificate = certificate;
            
            Conventions = conventions.Clone();

            var maxNumberOfContextsToKeepInGlobalStack = PlatformDetails.Is32Bits == false
                ? 1024
                : 256;

            ContextPool = new JsonContextPool(Conventions.MaxContextSizeToKeep, maxNumberOfContextsToKeepInGlobalStack, 1024);

            DefaultTimeout = Conventions.RequestTimeout;
            SecondBroadcastAttemptTimeout = conventions.SecondBroadcastAttemptTimeout;
            FirstBroadcastAttemptTimeout = conventions.FirstBroadcastAttemptTimeout;

            TopologyHash = Http.TopologyHash.GetTopologyHash(initialUrls);

            UpdateConnectionLimit(initialUrls);
        }

        ~RequestExecutor()
        {
            try
            {
                Dispose();
            }
#pragma warning disable CS0168
            catch (Exception e)
#pragma warning restore CS0168
            {
#if DEBUG
                Console.WriteLine($"Finalizer of {GetType()} got an exception:{Environment.NewLine}{e}");
#endif          
                // nothing we can do here
            }
        }

        public static RequestExecutor Create(string[] initialUrls, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            var executor = new RequestExecutor(databaseName, certificate, conventions, initialUrls);
            conventions.ForTestingPurposes?.OnBeforeTopologyUpdate?.Invoke(executor);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(initialUrls, GlobalApplicationIdentifier);
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
                            Url = url,
                            ServerRole = ServerNode.Role.Member
                        }
                    }
                }),
                TopologyEtag = InitialTopologyEtag,
                _disableTopologyUpdates = true,
                _disableClientConfigurationUpdates = true
            };
            executor._firstTopologyUpdate = executor.SingleTopologyUpdateAsync(initialUrls, GlobalApplicationIdentifier);
            return executor;
        }

        internal static RequestExecutor CreateForShortTermUse(string[] initialUrls, string databaseName, X509Certificate2 certificate, DocumentConventions conventions)
        {
            // This request executor doesn't call SingleTopologyUpdate so it won't attempt to reach the server of the url before it knows our certificate
            // It's topology will also not have cluster tags populated so it cannot be used long term for operations with SelectedNodeTag
            var urls = ValidateUrls(initialUrls, certificate);
            var nodes = urls.Select(u => new ServerNode
            {
                Database = databaseName,
                Url = u,
                ServerRole = ServerNode.Role.Member
            }).ToList();
            var executor = new RequestExecutor(databaseName, certificate, conventions, urls)
            {
                _nodeSelector = new NodeSelector(new Topology
                {
                    Etag = -1,
                    Nodes = nodes
                }),
                TopologyEtag = InitialTopologyEtag,
                _disableTopologyUpdates = true,
                _disableClientConfigurationUpdates = true
            };
            executor._firstTopologyUpdate = Task.CompletedTask;
            return executor;
        }

        protected virtual async Task UpdateClientConfigurationAsync(ServerNode serverNode)
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

                    await ExecuteAsync(serverNode, null, context, command, shouldRetry: false, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);

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

        public virtual async Task<bool> UpdateTopologyAsync(UpdateTopologyParameters parameters)
        {
            if (parameters is null)
                throw new ArgumentNullException(nameof(parameters));

            if (_disableTopologyUpdates)
                return false;

            if (Disposed)
                return false;

            //prevent double topology updates if execution takes too much time
            // --> in cases with transient issues
            var lockTaken = await _updateDatabaseTopologySemaphore.WaitAsync(parameters.TimeoutInMs).ConfigureAwait(false);
            if (lockTaken == false)
                return false;

            try
            {
                if (Disposed)
                    return false;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetDatabaseTopologyCommand(parameters.DebugTag, Conventions.SendApplicationIdentifier ? parameters.ApplicationIdentifier : null);
                    ForTestingPurposes?.SetCommandTimeout?.Invoke(command);

                    if (DefaultTimeout.HasValue && DefaultTimeout.Value > command.Timeout)
                        command.Timeout = DefaultTimeout.Value;

                    await ExecuteAsync(parameters.Node, null, context, command, shouldRetry: false, sessionInfo: null, token: CancellationToken.None).ConfigureAwait(false);
                    var topology = command.Result;

                    await DatabaseTopologyLocalCache.TrySavingAsync(_databaseName, TopologyHash, topology, Conventions, context, CancellationToken.None).ConfigureAwait(false);
                    
                    UpdateNodeSelector(topology, parameters.ForceUpdate);
                    
                    var urls = _nodeSelector.Topology.Nodes.Select(x => x.Url);
                    UpdateConnectionLimit(urls);

                    OnTopologyUpdatedInvoke(topology);
                }
            }
            // we want to throw here only if we are not disposed yet
            catch (Exception)
            {
                if (Disposed == false)
                    throw;

                return false;
            }
            finally
            {
                _updateDatabaseTopologySemaphore.Release();
            }
            return true;
        }

        protected void UpdateNodeSelector(Topology topology, bool forceUpdate)
        {
            if (_nodeSelector == null)
            {
                _nodeSelector = new NodeSelector(topology);

                if (Conventions.ReadBalanceBehavior == ReadBalanceBehavior.FastestNode)
                {
                    ForTestingPurposes?.OnBeforeScheduleSpeedTest?.Invoke(_nodeSelector);
                    _nodeSelector.ScheduleSpeedTest();
                }
            }
            else if (_nodeSelector.OnUpdateTopology(topology, forceUpdate))
            {
                DisposeAllFailedNodesTimers();
                if (Conventions.ReadBalanceBehavior == ReadBalanceBehavior.FastestNode && _nodeSelector.InSpeedTestPhase)
                {
                    _nodeSelector.ScheduleSpeedTest();
                }
            }
            TopologyEtag = _nodeSelector.Topology.Etag;
        }

        protected void DisposeAllFailedNodesTimers()
        {
            foreach (var failedNodesTimers in _failedNodesTimers)
            {
                if (_failedNodesTimers.TryRemove(failedNodesTimers.Key, out var status))
                    status.Value.Dispose();
            }
        }

        public void Execute<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            SessionInfo sessionInfo = null)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(command, context, sessionInfo, CancellationToken.None));
        }

        public Task ExecuteAsync<TResult>(
            RavenCommand<TResult> command,
            JsonOperationContext context,
            SessionInfo sessionInfo = null,
            CancellationToken token = default)
        {
            var topologyUpdate = _firstTopologyUpdate;

            if (topologyUpdate != null && topologyUpdate.Status == TaskStatus.RanToCompletion)
            {
                var (nodeIndex, chosenNode) = ChooseNodeForRequest(command, sessionInfo);
                return ExecuteAsync(chosenNode, nodeIndex, context, command, shouldRetry: true, sessionInfo: sessionInfo, token: token);
            }

            return UnlikelyExecuteAsync(command, context, topologyUpdate, sessionInfo, token);
        }

        public (int CurrentIndex, ServerNode CurrentNode) ChooseNodeForRequest<TResult>(RavenCommand<TResult> cmd, SessionInfo sessionInfo = null)
        {
            if (string.IsNullOrEmpty(cmd.SelectedNodeTag) == false)
            {
                return _nodeSelector.GetRequestedNode(cmd.SelectedNodeTag);
            }
            
            switch (Conventions.LoadBalanceBehavior)
            {
                case LoadBalanceBehavior.UseSessionContext:
                    if (sessionInfo?.CanUseLoadBalanceBehavior == true)
                        return _nodeSelector.GetNodeBySessionId(sessionInfo.SessionId);
                    break;
            }

            if (cmd.IsReadRequest == false)
            {
                return _nodeSelector.GetPreferredNode();
            }

            switch (Conventions.ReadBalanceBehavior)
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
            await WaitForTopologyUpdate(topologyUpdate).ConfigureAwait(false);

            var (currentIndex, currentNode) = ChooseNodeForRequest(command, sessionInfo);
            await ExecuteAsync(currentNode, currentIndex, context, command, true, sessionInfo, token).ConfigureAwait(false);
        }

        private async Task WaitForTopologyUpdate(Task topologyUpdate)
        {
            try
            {
                if (topologyUpdate == null ||
                    // if we previously had a topology error, let's see if we can refresh this
                    // can happen if the user tried a request to a db that didn't exist, created it, then tried immediately
                    topologyUpdate.IsFaulted)
                {
                    lock (this)
                    {
                        if (_firstTopologyUpdate == null || topologyUpdate == _firstTopologyUpdate)
                        {
                            if (_lastKnownUrls == null)
                            {
                                // shouldn't happen
                                throw new InvalidOperationException("No known topology and no previously known one, cannot proceed, likely a bug", topologyUpdate?.Exception);
                            }

                            if (_disableTopologyUpdates == false)
                                _firstTopologyUpdate = FirstTopologyUpdate(_lastKnownUrls, null);
                            else
                                _firstTopologyUpdate = SingleTopologyUpdateAsync(_lastKnownUrls);
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
        }

        internal static async void UpdateTopologyCallback(object state)
        {
            var requestExecutor = (RequestExecutor)state;

            var selector = requestExecutor._nodeSelector;
            if (selector == null || selector.Topology == null)
                return;
            
            // Fetch topologies from all nodes, the executor's topology will be updated to the most recent one
            foreach (var serverNode in selector.Topology.Nodes)
            {
                try
                {
                    if(serverNode.ServerRole != ServerNode.Role.Member)
                        continue;

                    await requestExecutor.UpdateTopologyAsync(new UpdateTopologyParameters(serverNode) {TimeoutInMs = 0, DebugTag = $"timer-callback-node-{serverNode.ClusterTag}"})
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Couldn't Update Topology from _updateTopologyTimer task when fetching from node {serverNode.ClusterTag}", e);
                }
            }
        }

        protected async Task SingleTopologyUpdateAsync(string[] initialUrls, Guid? applicationIdentifier = null)
        {
            if(Disposed)
                return;

            //fetch tag for each of the urls
            Topology topology = new Topology() {Nodes = new List<ServerNode>(), Etag = TopologyEtag};
            
            foreach (var url in initialUrls)
            {
                var serverNode = new ServerNode
                {
                    Url = url,
                    Database = _databaseName
                };

                try
                {
                    using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var command = new GetNodeInfoCommand(GlobalHttpClientTimeout);
                        await ExecuteAsync(serverNode, null, context, command, shouldRetry: false, sessionInfo: null, token: CancellationToken.None)
                            .ConfigureAwait(false);
                        serverNode.ClusterTag = command.Result.NodeTag;
                        serverNode.ServerRole = command.Result.ServerRole;
                    }
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
                    serverNode.ClusterTag = "!";
                    if(Logger.IsInfoEnabled)
                        Logger.Info($"Error occurred while attempting to fetch the Cluster Tag for {url} in {nameof(SingleTopologyUpdateAsync)}", e);
                }

                topology.Nodes.Add(serverNode);

                UpdateNodeSelector(topology, forceUpdate: true);
            }
            
            _lastKnownUrls = initialUrls;
        }

        protected async Task FirstTopologyUpdate(string[] initialUrls, Guid? applicationIdentifier = null)
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

                    await UpdateTopologyAsync(new UpdateTopologyParameters(serverNode)
                    {
                        TimeoutInMs = Timeout.Infinite,
                        DebugTag = "first-topology-update",
                        ApplicationIdentifier = applicationIdentifier
                    }).ConfigureAwait(false);

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
                    list.Add((url, e));
                }
            }

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = TopologyNodes?.ToList() ?? initialUrls.Select(url => new ServerNode
                {
                    Url = url,
                    Database = _databaseName,
                    ServerRole = ServerNode.Role.Member,
                    ClusterTag = "!"
                }).ToList(),
                Etag = TopologyEtag
            });

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                if (await TryLoadFromCacheAsync(context).ConfigureAwait(false))
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

        internal static string[] ValidateUrls(string[] initialUrls, X509Certificate2 certificate)
        {
            var cleanUrls = new string[initialUrls.Length];
            var requireHttps = certificate != null;
            for (var index = 0; index < initialUrls.Length; index++)
            {
                var url = initialUrls[index];
                if (Uri.TryCreate(url, UriKind.Absolute, out var uri) == false)
                    throw new InvalidOperationException($"'{url}' is not a valid url");

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
                    throw new InvalidOperationException($"The url {url} is using HTTP, but a certificate is specified, which require us to use HTTPS");

                throw new InvalidOperationException($"The url {url} is using HTTP, but other urls are using HTTPS, and mixing of HTTP and HTTPS is not allowed");
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

                _updateTopologyTimer = new WeakReferencingTimer(UpdateTopologyCallback, this, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
            }
        }

        protected virtual async Task<bool> TryLoadFromCacheAsync(JsonOperationContext context)
        {
            var cachedTopology = await DatabaseTopologyLocalCache.TryLoadAsync(_databaseName, TopologyHash, Conventions, context).ConfigureAwait(false);
            if (cachedTopology == null)
                return false;

            _nodeSelector = new NodeSelector(cachedTopology);
            TopologyEtag = InitialTopologyEtag;
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
            token.ThrowIfCancellationRequested();

            if (command.FailoverTopologyEtag == InitialTopologyEtag)
                command.FailoverTopologyEtag = _nodeSelector?.Topology?.Etag ?? InitialTopologyEtag;

            var request = CreateRequest(context, chosenNode, command, out string url);
            if (request == null)
                return;

            var noCaching = sessionInfo?.NoCaching ?? false;

            using (var cachedItem = GetFromCache(context, command, !noCaching, url, out string cachedChangeVector, out BlittableJsonReaderObject cachedValue))
            {
                if (cachedChangeVector != null)
                {
                    if (TryGetFromCache(context, command, cachedItem, cachedValue))
                        return;
                }

                if (sessionInfo?.AsyncCommandRunning ?? false)
                    ThrowInvalidConcurrentSessionUsage(command.GetType().Name, sessionInfo);

                SetRequestHeaders(sessionInfo, cachedChangeVector, request);

                command.NumberOfAttempts += 1;
                var attemptNum = command.NumberOfAttempts;
                OnBeforeRequest?.Invoke(this, new BeforeRequestEventArgs(_databaseName, url, request, attemptNum));
                var response = await SendRequestToServer(chosenNode, nodeIndex, context, command, shouldRetry, sessionInfo, request, url, token).ConfigureAwait(false);
                if (response == null) // the fail-over mechanism took care of this
                    return;

                var refreshTask = RefreshIfNeeded(chosenNode, response);
                command.StatusCode = response.StatusCode;

                var responseDispose = ResponseDisposeHandling.Automatic;
                try
                {
                    if (response.StatusCode == HttpStatusCode.NotModified)
                    {
                        OnSucceedRequest?.Invoke(this, new SucceedRequestEventArgs(_databaseName, url, response, request, attemptNum));

                        cachedItem.NotModified();

                        if (command.ResponseType == RavenCommandResponseType.Object)
                            command.SetResponse(context, cachedValue, fromCache: true);

                        return;
                    }

                    if (response.IsSuccessStatusCode == false)
                    {
                        if (await HandleUnsuccessfulResponse(chosenNode, nodeIndex, context, command, request, response, url, sessionInfo, shouldRetry, token).ConfigureAwait(false) == false)
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
                    
                    if (sessionInfo != null && response.Headers.TryGetValues(Constants.Headers.DatabaseClusterTransactionId, out var clusterTransactionId))
                    {
                        sessionInfo.ClusterTransactionId = clusterTransactionId.First();
                    }

                    OnSucceedRequest?.Invoke(this, new SucceedRequestEventArgs(_databaseName, url, response, request, attemptNum));
                    responseDispose = await command.ProcessResponse(context, Cache, response, url).ConfigureAwait(false);
                }
                finally
                {
                    if (responseDispose == ResponseDisposeHandling.Automatic)
                    {
                        response.Dispose();
                    }

                    await refreshTask.ConfigureAwait(false);
                }
            }
        }

        private Task RefreshIfNeeded(ServerNode chosenNode, HttpResponseMessage response)
        {
            var refreshTopology = response.GetBoolHeader(Constants.Headers.RefreshTopology) ?? false;
            var refreshClientConfiguration = response.GetBoolHeader(Constants.Headers.RefreshClientConfiguration) ?? false;

            var refreshTask = Task.CompletedTask;
            var refreshClientConfigurationTask = Task.CompletedTask;

            if (refreshTopology)
            {
                refreshTask = UpdateTopologyAsync(
                    new UpdateTopologyParameters(chosenNode)
                    {
                        TimeoutInMs = 0,
                        DebugTag = "refresh-topology-header"
                    });
            }

            if (refreshClientConfiguration)
            {
                refreshClientConfigurationTask = UpdateClientConfigurationAsync(chosenNode);
            }

            return Task.WhenAll(refreshTask, refreshClientConfigurationTask);
        }

        private async Task<HttpResponseMessage> SendRequestToServer<TResult>(
            ServerNode chosenNode,
            int? nodeIndex,
            JsonOperationContext context,
            RavenCommand<TResult> command,
            bool shouldRetry,
            SessionInfo sessionInfo,
            HttpRequestMessage request,
            string url,
            CancellationToken token)
        {
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
                            ForTestingPurposes?.DelayRequest?.Invoke();
                            return await SendAsync(chosenNode, command, sessionInfo, request, cts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException e)
                        {
                            if (cts.IsCancellationRequested && token.IsCancellationRequested == false) // only when we timed out
                            {
                                var timeoutException = new TimeoutException($"The request for {request.RequestUri} failed with timeout after {timeout}", e);
                                if (shouldRetry == false)
                                {
                                    if (command.FailedNodes == null)
                                        command.FailedNodes = new Dictionary<ServerNode, Exception>();

                                    command.FailedNodes[chosenNode] = timeoutException;
                                    throw timeoutException;
                                }

                                if (sessionInfo != null)
                                    sessionInfo.AsyncCommandRunning = false;

                                if (await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, null, timeoutException, sessionInfo, shouldRetry,
                                        requestContext: null, token: token).ConfigureAwait(false) == false)
                                    ThrowFailedToContactAllNodes(command, request);

                                return null;
                            }

                            throw;
                        }
                    }
                }
                else
                {
                    return await SendAsync(chosenNode, command, sessionInfo, request, token).ConfigureAwait(false);
                }
            }
            catch (HttpRequestException e) // server down, network down
            {
                using (GetContext(out var requestContext))
                {
                    if (e.InnerException is SocketException se && ShouldRemoveHttpClient(se))
                    {
                        if (requestContext.HttpClientRemoved == false)
                        {
                            if (TryRemoveHttpClient())
                                requestContext.HttpClientRemoved = true;
                        }
                        else
                        {
                            requestContext.HttpClientRemoved = false;
                        }
                    }

                    if (shouldRetry == false)
                        throw;

                    if (sessionInfo != null)
                        sessionInfo.AsyncCommandRunning = false;

                    if (await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, null, e, sessionInfo, shouldRetry, requestContext, token)
                            .ConfigureAwait(false) == false)
                    {
                        ThrowIfClientException(e);
                        ThrowFailedToContactAllNodes(command, request);
                    }

                    return null;
                }
            }
            finally
            {
                if (sessionInfo != null)
                    sessionInfo.AsyncCommandRunning = false;
            }
        }

        private async Task<HttpResponseMessage> SendAsync<TResult>(
            ServerNode chosenNode,
            RavenCommand<TResult> command,
            SessionInfo sessionInfo,
            HttpRequestMessage request,
            CancellationToken token)
        {
            var preferredTask = command.SendAsync(HttpClient, request, token);
            if (ShouldExecuteOnAll(chosenNode, command))
            {
                await ExecuteOnAllToFigureOutTheFastest(chosenNode, command, preferredTask, token).ConfigureAwait(false);
            }

            var response = await preferredTask.ConfigureAwait(false);

            // PERF: The reason to avoid rechecking every time is that servers wont change so rapidly
            //       and therefore we dimish its cost by orders of magnitude just doing it
            //       once in a while. We dont care also about the potential race conditions that may happen
            //       here mainly because the idea is to have a lax mechanism to recheck that is at least
            //       orders of magnitude faster than currently. 
            if (chosenNode.ShouldUpdateServerVersion())
            {
                if (TryGetServerVersion(response, out var serverVersion))
                    chosenNode.UpdateServerVersion(serverVersion);
            }

            LastServerVersion = chosenNode.LastServerVersion;

            return response;
        }

        private void SetRequestHeaders(SessionInfo sessionInfo, string cachedChangeVector, HttpRequestMessage request)
        {
            if (cachedChangeVector != null)
                request.Headers.TryAddWithoutValidation(Constants.Headers.IfNoneMatch, $"\"{cachedChangeVector}\"");

            if (_disableClientConfigurationUpdates == false)
                request.Headers.TryAddWithoutValidation(Constants.Headers.ClientConfigurationEtag, $"\"{ClientConfigurationEtag.ToInvariantString()}\"");

            if (sessionInfo?.LastClusterTransactionIndex != null)
            {
                request.Headers.TryAddWithoutValidation(Constants.Headers.LastKnownClusterTransactionIndex, sessionInfo.LastClusterTransactionIndex.ToString());
            }

            if (_disableTopologyUpdates == false)
                request.Headers.TryAddWithoutValidation(_topologyHeaderName, $"\"{TopologyEtag.ToInvariantString()}\"");

            if (request.Headers.Contains(Constants.Headers.ClientVersion) == false)
                request.Headers.Add(Constants.Headers.ClientVersion, _localClientVersion ?? ClientVersion);
        }

        private bool TryGetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, HttpCache.ReleaseCacheItem cachedItem, BlittableJsonReaderObject cachedValue)
        {
            var aggressiveCacheOptions = AggressiveCaching.Value;
            if (aggressiveCacheOptions != null &&
                cachedItem.Age < aggressiveCacheOptions.Duration &&
                (cachedItem.MightHaveBeenModified == false || aggressiveCacheOptions.Mode != AggressiveCacheMode.TrackChanges) &&
                command.CanCacheAggressively)
            {
                if ((cachedItem.Item.Flags & HttpCache.ItemFlags.NotFound) != HttpCache.ItemFlags.None)
                {
                    // if this is a cached delete, we only respect it if it _came_ from an aggressively cached
                    // block, otherwise, we'll run the request again
                    if ((cachedItem.Item.Flags & HttpCache.ItemFlags.AggressivelyCached) == HttpCache.ItemFlags.AggressivelyCached)
                    {
                        command.SetResponse(context, cachedValue, fromCache: true);
                        return true;
                    }
                }
                else
                {
                    command.SetResponse(context, cachedValue, fromCache: true);
                    return true;
                }
            }

            return false;
        }

        private IDisposable GetContext(out RequestContext context)
        {
            context = _requestContexts.Value;
            var isNull = context == null;
            if (isNull)
                context = _requestContexts.Value = new RequestContext();

            return isNull ? _clearContext : _doNotClearContext;
        }

        private static bool TryGetServerVersion(HttpResponseMessage response, out string version)
        {
            if (response.Headers.TryGetValues(Constants.Headers.ServerVersion, out var values) == false)
            {
                version = null;
                return false;
            }

            version = values.FirstOrDefault();
            return version != null;
        }

        private void ThrowFailedToContactAllNodes<TResult>(RavenCommand<TResult> command, HttpRequestMessage request)
        {
            if (command.FailedNodes == null || command.FailedNodes.Count == 0) //precaution, should never happen at this point
                throw new InvalidOperationException("Received unsuccessful response and couldn't recover from it. Also, no record of exceptions per failed nodes. " +
                                                    "This is weird and should not happen.");

            if (command.FailedNodes.Count == 1)
                ExceptionDispatchInfo.Capture(command.FailedNodes.First().Value).Throw();

            var message = $"Tried to send '{command.GetType().Name}' request via `{request.Method} {request.RequestUri.PathAndQuery}` " +
                          $"to all configured nodes in the topology, none of the attempt succeeded. {Environment.NewLine}";

            if (_topologyTakenFromNode != null)
                message += $"I was able to fetch {_topologyTakenFromNode.Database} topology from {_topologyTakenFromNode.Url}.{Environment.NewLine}";

            var nodes = _nodeSelector?.Topology?.Nodes;
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
            if (Conventions.ReadBalanceBehavior != ReadBalanceBehavior.FastestNode)
                return false;

            var selector = _nodeSelector;

            return selector != null &&
                   selector.InSpeedTestPhase &&
                   selector.Topology?.Nodes?.Count > 1 &&
                   command.IsReadRequest &&
                   command.ResponseType == RavenCommandResponseType.Object &&
                   chosenNode != null &&
                   command is IBroadcast == false;
        }

        internal static readonly Task<HttpRequestMessage> NeverEndingRequest = new TaskCompletionSource<HttpRequestMessage>(TaskCreationOptions.RunContinuationsAsynchronously).Task;

        private async Task ExecuteOnAllToFigureOutTheFastest<TResult>(ServerNode chosenNode, RavenCommand<TResult> command, Task<HttpResponseMessage> preferredTask,
            CancellationToken token = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            long numberOfFailedTasks = 0;

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
                    var request = CreateRequest(tmpCtx, nodes[i], command, out _);
                    SetRequestHeaders(null, null, request);

                    Interlocked.Increment(ref NumberOfServerRequests);

                    var copy = disposable;
                    tasks[i] = command.SendAsync(HttpClient, request, cts.Token).ContinueWith(x =>
                    {
                        ForTestingPurposes?.ExecuteOnAllToFigureOutTheFastestOnTaskCompletion?.Invoke(x);

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
                            copy.Dispose();
                        }
                    }, cts.Token);
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref numberOfFailedTasks);
                    // nothing we can do about it
                    tasks[i] = NeverEndingRequest;
                    disposable?.Dispose();
                }
            }

            ForTestingPurposes?.ExecuteOnAllToFigureOutTheFastestOnBeforeWait?.Invoke(tasks);

            while (Interlocked.Read(ref numberOfFailedTasks) < tasks.Length)
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

                cts.Cancel();

                // we need to await all the tasks to avoid them running after e.g. session is disposed
                foreach (var task in tasks)
                {
                    if (task.IsCompleted || task.IsCanceled || task.IsFaulted)
                        continue;

                    if (task == NeverEndingRequest)
                        continue;

                    try
                    {
                        await task.ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignore
                    }
                }

                return;
            }

            // we can reach here if the number of failed task equal to the number
            // of the nodes, in which case we have nothing to do
        }

        private static void ThrowTimeoutTooLarge(TimeSpan? timeout)
        {
            throw new InvalidOperationException($"Maximum request timeout is set to '{GlobalHttpClientTimeout}' but was '{timeout}'.");
        }

        private HttpCache.ReleaseCacheItem GetFromCache<TResult>(JsonOperationContext context, RavenCommand<TResult> command, bool useCache, string url, out string cachedChangeVector, out BlittableJsonReaderObject cachedValue)
        {
            if (useCache && command.CanCache && command.IsReadRequest && command.ResponseType == RavenCommandResponseType.Object)
            {
                return Cache.Get(context, url, out cachedChangeVector, out cachedValue);
            }

            cachedChangeVector = null;
            cachedValue = null;
            return new HttpCache.ReleaseCacheItem();
        }

        private string _localClientVersion;

        internal IDisposable UsingClientVersion(string clientVersion)
        {
            _localClientVersion = clientVersion;

            return new DisposableAction(() => _localClientVersion = null);
        }

        public static readonly string ClientVersion = RavenVersionAttribute.Instance.AssemblyVersion;

        internal HttpRequestMessage CreateRequest<TResult>(JsonOperationContext ctx, ServerNode node, RavenCommand<TResult> command, out string url)
        {
            var request = command.CreateRequest(ctx, node, out url);
            if (request == null)
                return null;

            var builder = new UriBuilder(url);

            if (command is IRaftCommand raftCommand)
            {
                Debug.Assert(raftCommand.RaftUniqueRequestId != null, $"Forget to create an id for {command.GetType()}?");

                var raftRequestString = "raft-request-id=" + raftCommand.RaftUniqueRequestId;
                builder.Query = builder.Query?.Length > 1 ? $"{builder.Query.Substring(1)}&{raftRequestString}" : raftRequestString;
            }

            if (ShouldBroadcast(command))
            {
                command.SetTimeout(command.Timeout ?? _firstBroadcastAttemptTimeout);
            }

            if (Conventions.HttpVersion != null)
                request.Version = Conventions.HttpVersion;

            request.RequestUri = builder.Uri;

            return request;
        }

        public event Action<StringBuilder> AdditionalErrorInformation;

        private async Task<bool> HandleUnsuccessfulResponse<TResult>(ServerNode chosenNode, int? nodeIndex, JsonOperationContext context, RavenCommand<TResult> command,
            HttpRequestMessage request, HttpResponseMessage response, string url, SessionInfo sessionInfo, bool shouldRetry, CancellationToken token = default)
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
                    var msg = await TryGetResponseOfError(response).ConfigureAwait(false);
                    var builder = new StringBuilder("Forbidden access to ").
                        Append(chosenNode.Database).Append("@").Append(chosenNode.Url).Append(", ");
                    if (Certificate == null)
                    {
                        builder.Append("a certificate is required. ");
                    }
                    else if (Certificate.HasPrivateKey)
                    {
                        builder.Append(Certificate.FriendlyName).Append(" does not have permission to access it or is unknown. ");
                    }
                    else
                    {
                        builder.Append("The certificate ").Append(Certificate.FriendlyName)
                            .Append(" contains no private key. Constructing the certificate with the 'X509KeyStorageFlags.MachineKeySet' flag may solve this problem. ");
                    }
                    builder.Append("Method: ").Append(request.Method).Append(", Request: ").AppendLine(request.RequestUri.ToString()).Append(msg);
                    throw new AuthorizationException(builder.ToString());
                case HttpStatusCode.Gone: // request not relevant for the chosen node - the database has been moved to a different one
                    if (shouldRetry == false)
                        return false;

                    if (nodeIndex != null)
                        _nodeSelector.OnFailedRequest(nodeIndex.Value);

                    if (command.FailedNodes == null)
                        command.FailedNodes = new Dictionary<ServerNode, Exception>();

                    if (command.IsFailedWithNode(chosenNode) == false)
                        command.FailedNodes[chosenNode] = new UnsuccessfulRequestException($"Request to '{request.RequestUri}' ({request.Method}) is not relevant for this node anymore.");

                    var (index, node) = ChooseNodeForRequest(command, sessionInfo);

                    if (command.FailedNodes.ContainsKey(node))
                    {
                        // we tried all the nodes, let's try to update topology and retry one more time
                        var success = await UpdateTopologyAsync(new UpdateTopologyParameters(chosenNode) { TimeoutInMs = 60 * 1000, ForceUpdate = true, DebugTag = "handle-unsuccessful-response" }).ConfigureAwait(false);

                        if (success == false)
                            return false;

                        command.FailedNodes.Clear(); // we just updated the topology
                        (index, node) = ChooseNodeForRequest(command, sessionInfo);

                        await ExecuteAsync(node, index, context, command, shouldRetry: false, sessionInfo: sessionInfo, token: token).ConfigureAwait(false);
                        return true;
                    }

                    await ExecuteAsync(node, index, context, command, shouldRetry: true, sessionInfo: sessionInfo, token: token).ConfigureAwait(false);
                    return true;

                case HttpStatusCode.GatewayTimeout:
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    return await HandleServerDown(url, chosenNode, nodeIndex, context, command, request, response, null, sessionInfo, shouldRetry, requestContext: null, token: token).ConfigureAwait(false);

                case HttpStatusCode.Conflict:
                    await HandleConflict(context, response).ConfigureAwait(false);
                    break;

                case (HttpStatusCode)425: // TooEarly

                    if (shouldRetry == false)
                        return false;

                    if (nodeIndex != null)
                        _nodeSelector.OnFailedRequest(nodeIndex.Value);

                    if (command.FailedNodes == null)
                        command.FailedNodes = new Dictionary<ServerNode, Exception>();

                    if (command.IsFailedWithNode(chosenNode) == false)
                        command.FailedNodes[chosenNode] = new UnsuccessfulRequestException($"Request to '{request.RequestUri}' ({request.Method}) is processing and not yet available on that node.");

                    var nextNode = ChooseNodeForRequest(command, sessionInfo);

                    await ExecuteAsync(nextNode.CurrentNode, nextNode.CurrentIndex, context, command, shouldRetry: true, sessionInfo: sessionInfo, token: token).ConfigureAwait(false);

                    _nodeSelector.RestoreNodeIndex(chosenNode);

                    return true;

                default:
                    command.OnResponseFailure(response);
                    await ExceptionDispatcher.Throw(context, response, AdditionalErrorInformation).ConfigureAwait(false);
                    break;
            }
            return false;
        }

        private static async Task<string> TryGetResponseOfError(HttpResponseMessage response)
        {
            try
            {
                return (await response.Content.ReadAsStringAsync().ConfigureAwait(false));
            }
            catch (Exception e)
            {
                return "Could not read request: " + e.Message;
            }
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
            HttpRequestMessage request, HttpResponseMessage response, Exception e, SessionInfo sessionInfo, bool shouldRetry, RequestContext requestContext = null, CancellationToken token = default)
        {
            command.FailedNodes ??= new Dictionary<ServerNode, Exception>();

            var exception = await ReadExceptionFromServer(context, request, response, e).ConfigureAwait(false);
            if (exception is RavenTimeoutException { FailImmediately: true })
                throw exception;

            command.FailedNodes[chosenNode] = exception;

            if (nodeIndex.HasValue == false)
            {
                // we executed request over a node not in the topology. This means no failover...
                return false;
            }

            if (_nodeSelector == null)
            {
                SpawnHealthChecks(chosenNode, nodeIndex.Value);
                return false;
            }

            // As the server is down, we discard the server version to ensure we update when it goes up. 
            chosenNode.DiscardServerVersion();

            _nodeSelector.OnFailedRequest(nodeIndex.Value);

            if (ShouldBroadcast(command))
            {
                command.Result = await Broadcast(command, sessionInfo, token).ConfigureAwait(false);
                return true;
            }

            SpawnHealthChecks(chosenNode, nodeIndex.Value);

            var (currentIndex, currentNode) = ChooseNodeForRequest(command, sessionInfo);
            var topologyEtag = _nodeSelector.Topology?.Etag ?? -2;

            if (command.FailoverTopologyEtag != topologyEtag)
            {
                command.FailedNodes.Clear();
                command.FailoverTopologyEtag = topologyEtag;
            }

            if (command.FailedNodes.ContainsKey(currentNode))
            {
                if (requestContext == null || requestContext.HttpClientRemoved == false)
                    return false; //we tried all the nodes...nothing left to do
            }

            OnFailedRequestInvoke(url, e, response, request);

            await ExecuteAsync(currentNode, currentIndex, context, command, shouldRetry, sessionInfo: sessionInfo, token: token).ConfigureAwait(false);

            return true;
        }

        private bool ShouldBroadcast<TResult>(RavenCommand<TResult> command)
        {
            if (command is IBroadcast == false)
                return false;

            if (TopologyNodes == null ||
                TopologyNodes.Count < 2)
                return false;

            return true;
        }

        private class BroadcastState<TResult>
        {
            public RavenCommand<TResult> Command;
            public int Index;
            public ServerNode Node;
            public IDisposable ReturnContext;
        }

        private async Task<TResult> Broadcast<TResult>(RavenCommand<TResult> command, SessionInfo sessionInfo, CancellationToken token)
        {
            var broadcastCommand = command as IBroadcast;
            if (broadcastCommand == null)
                throw new InvalidOperationException("You can broadcast only commands that implement 'IBroadcast'.");

            var failedNodes = command.FailedNodes;
            command.FailedNodes = new Dictionary<ServerNode, Exception>(); // clear the current failures

            using (var broadcastCts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                var broadcastTasks = new Dictionary<Task, BroadcastState<TResult>>();

                try
                {
                    SendToAllNodes(broadcastTasks, sessionInfo, broadcastCommand, broadcastCts.Token);

                    return await WaitForBroadcastResult(command, broadcastTasks, broadcastCts).ConfigureAwait(false);
                }
                finally
                {
                    foreach (var broadcastState in broadcastTasks)
                    {
                        // we can't dispose it right away, we need for the task to be completed in order not to have a concurrent usage of the context.
                        broadcastState.Key?.ContinueWith(_ =>
                        {
                            if (broadcastState.Key.IsFaulted || broadcastState.Key.IsCanceled)
                            {
                                var index = broadcastState.Value.Index;
                                var node = _nodeSelector.Topology.Nodes[index];
                                if (failedNodes.ContainsKey(node))
                                {
                                    // if other node succeed in broadcast we need to send health checks to the original failed node
                                    SpawnHealthChecks(node, index);
                                }
                            }

                            broadcastState.Value.ReturnContext.Dispose();
                        }, TaskContinuationOptions.ExecuteSynchronously);
                    }
                }
            }
        }

        private async Task<TResult> WaitForBroadcastResult<TResult>(RavenCommand<TResult> command, Dictionary<Task, BroadcastState<TResult>> tasks, CancellationTokenSource broadcastCts)
        {
            while (tasks.Count > 0)
            {
                var completed = await Task.WhenAny(tasks.Keys).ConfigureAwait(false);
                if (completed.IsCanceled || completed.IsFaulted)
                {
                    var failed = tasks[completed];
                    var node = _nodeSelector.Topology.Nodes[failed.Index];

                    command.FailedNodes[node] = completed.Exception?.ExtractSingleInnerException() ?? new UnsuccessfulRequestException(failed.Node.Url);

                    _nodeSelector.OnFailedRequest(failed.Index);
                    SpawnHealthChecks(node, failed.Index);

                    tasks.Remove(completed);
                    continue;
                }

                broadcastCts.Cancel(throwOnFirstException: false);

                _nodeSelector.RestoreNodeIndex(tasks[completed].Node);
                return tasks[completed].Command.Result;
            }

            var ae = new AggregateException(command.FailedNodes.Select(x => new UnsuccessfulRequestException(x.Key.Url, x.Value)));
            throw new AllTopologyNodesDownException($"Broadcasting {command.GetType()} failed.", ae);
        }

        private void SendToAllNodes<TResult>(Dictionary<Task, BroadcastState<TResult>> tasks, SessionInfo sessionInfo, IBroadcast command, CancellationToken token)
        {
            for (var index = 0; index < _nodeSelector.Topology.Nodes.Count; index++)
            {
                var state = new BroadcastState<TResult>
                {
                    ReturnContext = ContextPool.AllocateOperationContext(out JsonOperationContext ctx),
                    Index = index,
                    Node = _nodeSelector.Topology.Nodes[index],
                    Command = (RavenCommand<TResult>)command.PrepareToBroadcast(ctx, Conventions)
                };

                state.Command.SetTimeout(_secondBroadcastAttemptTimeout);

                var task = ExecuteAsync(state.Node, null, ctx, state.Command, shouldRetry: false, sessionInfo, token);
                tasks.Add(task, state);
            }
        }

        public async Task<ServerNode> HandleServerNotResponsive(string url, ServerNode chosenNode, int nodeIndex, Exception e)
        {
            SpawnHealthChecks(chosenNode, nodeIndex);
            _nodeSelector?.OnFailedRequest(nodeIndex);
            var (_, serverNode) = await GetPreferredNode().ConfigureAwait(false);

            if (_disableTopologyUpdates)
            {
                await PerformHealthCheck(chosenNode, nodeIndex).ConfigureAwait(false);
            }
            else
            {
                await UpdateTopologyAsync(new UpdateTopologyParameters(serverNode) { TimeoutInMs = 0, ForceUpdate = true, DebugTag = "handle-server-not-responsive" }).ConfigureAwait(false);
            }

            OnFailedRequestInvoke(url, e);
            return serverNode;
        }

        private void SpawnHealthChecks(ServerNode chosenNode, int nodeIndex)
        {
            if (_nodeSelector?.Topology.Nodes.Count < 2)
                return;

            var nodeStatus = new Lazy<NodeStatus>(() =>
            {
                var s = new NodeStatus(this, nodeIndex, chosenNode);
                s.StartTimer();

                return s;
            });

            var status = _failedNodesTimers.GetOrAdd(chosenNode, nodeStatus);
            if (status == nodeStatus)
            {
                var value = status.Value; // materialize
                return;
            }
        }

        internal Task CheckNodeStatusNow(string tag)
        {
            var copy = TopologyNodes;
            if (copy == null)
                throw new ArgumentException("There is no cluster topology available.");

            int i;
            for (i = 0; i < copy.Count; i++)
            {
                if (copy[i].ClusterTag == tag)
                    break;
            }
            if (i == copy.Count)
            {
                throw new ArgumentException($"Node {tag} was not found in the cluster topology.");
            }

            var nodeStatus = new NodeStatus(this, i, copy[i]);
            return CheckNodeStatusCallback(nodeStatus).ContinueWith(t => nodeStatus.Dispose());
        }

        private async Task CheckNodeStatusCallback(NodeStatus nodeStatus)
        {
            // In some cases, race conditions may occur with a recently changed topology and a failed node.
            // We still should check the node's health, and if healthy, remove its timer and restore its index.
            int nodeIndex;
            ServerNode serverNode;
            Lazy<NodeStatus> status;

            try
            {
                (nodeIndex, serverNode) = _nodeSelector.GetRequestedNode(nodeStatus.Node.ClusterTag);
            }
            catch (Exception e) when (e is DatabaseDoesNotExistException or RequestedNodeUnavailableException)
            {
                // There are no nodes in the topology or could not find requested node. Nothing we can do here
                if (_failedNodesTimers.TryRemove(nodeStatus.Node, out status))
                    status.Value.Dispose();

                return;
            }

            if (serverNode == null)
                return;

            try
            {
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        await PerformHealthCheck(serverNode, nodeIndex, context).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"{serverNode.ClusterTag} is still down", e);

                        if (_failedNodesTimers.TryGetValue(nodeStatus.Node, out status))
                            status.Value.UpdateTimer();

                        return;// will wait for the next timer call
                    }

                    if (_failedNodesTimers.TryRemove(nodeStatus.Node, out status))
                        status.Value.Dispose();

                    _nodeSelector?.RestoreNodeIndex(serverNode);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to check node topology, will ignore this node until next topology update", e);
            }
        }

        protected virtual async Task PerformHealthCheck(ServerNode serverNode, int nodeIndex)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await PerformHealthCheck(serverNode, nodeIndex, context).ConfigureAwait(false);
            }
        }
        protected virtual async Task PerformHealthCheck(ServerNode serverNode, int nodeIndex, JsonOperationContext context)
        {
            try
            {
                if (_useOldFailureCheckOperation == null || _useOldFailureCheckOperation.Contains(serverNode.Url) == false)
                {
                    await ExecuteAsync(serverNode, nodeIndex, context, FailureCheckOperation.GetCommand(Conventions, context), shouldRetry: false, sessionInfo: null,
                        token: CancellationToken.None).ConfigureAwait(false);
                }
                else
                {
                    await ExecuteOldHealthCheck().ConfigureAwait(false);
                }
            }
            catch (ClientVersionMismatchException e) when (e.Message.Contains(nameof(RouteNotFoundException)))
            {
                Interlocked.CompareExchange(ref _useOldFailureCheckOperation, new ConcurrentSet<string>(), null);
                // ReSharper disable once PossibleNullReferenceException
                _useOldFailureCheckOperation.Add(serverNode.Url);
                await ExecuteOldHealthCheck().ConfigureAwait(false);
            }

            Task ExecuteOldHealthCheck()
            {
                return ExecuteAsync(serverNode, nodeIndex, context, BackwardCompatibilityFailureCheckOperation.GetCommand(Conventions, context), shouldRetry: false,
                    sessionInfo: null, token: CancellationToken.None);
            }
        }

        private static async Task<Exception> ReadExceptionFromServer(JsonOperationContext context, HttpRequestMessage request, HttpResponseMessage response, Exception e)
        {
            if (response != null)
            {
                using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                using (var ms = new MemoryStream()) // todo: have a pool of those
                {
                    await stream.CopyToAsync(ms).ConfigureAwait(false);
                    try
                    {
                        ms.Position = 0;
                        using (var responseJson = await context.ReadForMemoryAsync(ms, "RequestExecutor/HandleServerDown/ReadResponseContent").ConfigureAwait(false))
                        {
                            return ExceptionDispatcher.Get(responseJson, response.StatusCode, e);
                        }
                    }
                    catch
                    {
                        using (var streamReader = new StreamReader(ms))
                        {
                            // we failed to parse the error
                            ms.Position = 0;
                            return ExceptionDispatcher.Get(
                                new ExceptionDispatcher.ExceptionSchema
                                {
                                    Url = request.RequestUri.ToString(),
                                    Message = "Got unrecognized response from the server",
                                    Error = await streamReader.ReadToEndAsync().ConfigureAwait(false),
                                    Type = "Unparseable Server Response"
                                }, response.StatusCode, e);
                        }
                    }
                }
            }

            // this would be connections that didn't have response, such as "couldn't connect to remote server"
            return ExceptionDispatcher.Get(new ExceptionDispatcher.ExceptionSchema
            {
                Url = request.RequestUri.ToString(),
                Message = e.Message,
                Error = $"An exception occurred while contacting {request.RequestUri}.{Environment.NewLine}{e}.",
                Type = e.GetType().FullName
            }, HttpStatusCode.ServiceUnavailable, e);
        }

        protected Task _firstTopologyUpdate;
        protected string[] _lastKnownUrls;
        private readonly DisposeOnce<ExceptionRetry> _disposeOnceRunner;
        protected bool Disposed => _disposeOnceRunner.Disposed;

        public static bool HasServerCertificateCustomValidationCallback => _serverCertificateCustomValidationCallback?.Length > 0;

        public virtual void Dispose()
        {
            if (_disposeOnceRunner.Disposed)
                return;

            _disposeOnceRunner.Dispose();
        }

        public static HttpClientHandler CreateHttpMessageHandler(X509Certificate2 certificate, bool setSslProtocols, bool useCompression, bool hasExplicitlySetCompressionUsage = false, TimeSpan? pooledConnectionLifetime = null, TimeSpan? pooledConnectionIdleTimeout = null)
        {
            HttpClientHandler httpMessageHandler;

            try
            {
                httpMessageHandler = new HttpClientHandler
                {
                    MaxConnectionsPerServer = DefaultConnectionLimit
                };
            }
            catch (NotImplementedException)
            {
                httpMessageHandler = new HttpClientHandler();
            }

            HttpClientHandlerHelper.Configure(httpMessageHandler, pooledConnectionLifetime, pooledConnectionIdleTimeout);

            if (httpMessageHandler.SupportsAutomaticDecompression)
            {
                httpMessageHandler.AutomaticDecompression =
                    useCompression ?
                        DecompressionMethods.GZip | DecompressionMethods.Deflate
                        : DecompressionMethods.None;
            }
            else if (useCompression && hasExplicitlySetCompressionUsage)
            {
                throw new NotSupportedException("HttpClient implementation for the current platform does not support request compression.");
            }

            if (ServerCertificateCustomValidationCallbackRegistrationException == null)
                httpMessageHandler.ServerCertificateCustomValidationCallback += OnServerCertificateCustomValidationCallback;

            if (certificate != null)
            {
                if (httpMessageHandler.ClientCertificates == null)
                    throw new NotSupportedException($"{typeof(HttpClientHandler)} does not support {nameof(httpMessageHandler.ClientCertificates)}. Setting the UseNativeHttpHandler property in project settings to false may solve the issue.");
                
                httpMessageHandler.ClientCertificates.Add(certificate);
                try
                {
                    if (setSslProtocols)
                        httpMessageHandler.SslProtocols = TcpUtils.SupportedSslProtocols;
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
#if NETCOREAPP3_1_OR_GREATER
            var httpPooledConnectionLifetime = Conventions.HttpPooledConnectionLifetime;
            var httpPooledConnectionIdleTimeout = Conventions.HttpPooledConnectionIdleTimeout;
#else
            TimeSpan? httpPooledConnectionLifetime = null;
            TimeSpan? httpPooledConnectionIdleTimeout = null;
#endif

            var httpMessageHandler = CreateHttpMessageHandler(Certificate,
                setSslProtocols: true,
                useCompression: Conventions.UseCompression,
                hasExplicitlySetCompressionUsage: Conventions.HasExplicitlySetCompressionUsage,
                httpPooledConnectionLifetime,
                httpPooledConnectionIdleTimeout
            );

            var httpClient = Conventions.CreateHttpClient(httpMessageHandler);
            httpClient.Timeout = GlobalHttpClientTimeout;

            return httpClient;
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

        private static readonly ConcurrentSet<string> UpdatedConnectionLimitUrls = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private static void UpdateConnectionLimit(IEnumerable<string> urls)
        {
            foreach (var url in urls)
            {
                if (UpdatedConnectionLimitUrls.TryAdd(url) == false)
                    continue;

                try
                {
#pragma warning disable SYSLIB0014 // Type or member is obsolete
                    var servicePoint = ServicePointManager.FindServicePoint(new Uri(url));
#pragma warning restore SYSLIB0014 // Type or member is obsolete
                    servicePoint.ConnectionLimit = DefaultConnectionLimit;
                    servicePoint.MaxIdleTime = -1;
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to set the connection limit for url: {url}", e);
                }
            }
        }

        private static RemoteCertificateValidationCallback[] _serverCertificateCustomValidationCallback = Array.Empty<RemoteCertificateValidationCallback>();
        private static readonly object _locker = new object();

        public static event RemoteCertificateValidationCallback RemoteCertificateValidationCallback
        {
            add
            {
                if (ServerCertificateCustomValidationCallbackRegistrationException != null)
                    ThrowRemoteCertificateValidationCallbackRegistrationException();

                lock (_locker)
                {
                    _serverCertificateCustomValidationCallback = _serverCertificateCustomValidationCallback.Concat(new[] { value }).ToArray();
                }
            }

            remove
            {
                if (ServerCertificateCustomValidationCallbackRegistrationException != null)
                    ThrowRemoteCertificateValidationCallbackRegistrationException();

                lock (_locker)
                {
                    _serverCertificateCustomValidationCallback = _serverCertificateCustomValidationCallback.Except(new[] { value }).ToArray();
                }
            }
        }

        private static void ThrowRemoteCertificateValidationCallbackRegistrationException()
        {
            throw new PlatformNotSupportedException(
                $"Cannot register {nameof(RemoteCertificateValidationCallback)}. {ServerCertificateCustomValidationCallbackRegistrationException.Message}",
                ServerCertificateCustomValidationCallbackRegistrationException);
        }

        internal static bool OnServerCertificateCustomValidationCallback(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors errors)
        {
            var onServerCertificateCustomValidationCallback = _serverCertificateCustomValidationCallback;
            if (onServerCertificateCustomValidationCallback == null ||
                onServerCertificateCustomValidationCallback.Length == 0)
            {
                if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) == SslPolicyErrors.RemoteCertificateNameMismatch)
                    ThrowCertificateNameMismatchException(sender, cert);

                return errors == SslPolicyErrors.None;
            }

            for (var i = 0; i < onServerCertificateCustomValidationCallback.Length; i++)
            {
                var result = onServerCertificateCustomValidationCallback[i](sender, cert, chain, errors);
                if (result)
                    return true;
            }

            if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) == SslPolicyErrors.RemoteCertificateNameMismatch)
                ThrowCertificateNameMismatchException(sender, cert);

            return false;
        }

        private static void ThrowCertificateNameMismatchException(object sender, X509Certificate cert)
        {
            var cert2 = cert as X509Certificate2 ?? new X509Certificate2(cert);
            var cn = cert2.Subject;
            var san = new List<string>();
            const string sanOid = "2.5.29.17";

            foreach (X509Extension extension in cert2.Extensions)
            {
                if (extension.Oid.Value.Equals(sanOid) == false)
                    continue;
                var asnData = new AsnEncodedData(extension.Oid, extension.RawData);
                san.Add(asnData.Format(false));
            }

            string hostname = ConvertSenderObjectToHostname(sender);

            // could not figure out hostname so instead let's throw a generic error
            if (string.IsNullOrEmpty(hostname))
                throw new CertificateNameMismatchException(
                    $"The hostname of the server URL must match one of the CN or SAN properties of the server certificate: {cn}, {string.Join(", ", san)}");

            throw new CertificateNameMismatchException(
                $"You are trying to contact host {hostname} but the hostname must match one of the CN or SAN properties of the server certificate: {cn}, {string.Join(", ", san)}");
        }

        public static string ConvertSenderObjectToHostname(object sender)
        {
            // The sender parameter passed to the RemoteCertificateValidationCallback can be a host string name or an object derived
            // from WebRequest. When using WebSockets, the sender parameter will be of type SslStream, but we cannot extract the
            // hostname from there so returning null.

            string hostname;
            switch (sender)
            {
                case HttpRequestMessage message:
                    hostname = message.RequestUri.DnsSafeHost;
                    break;

                case string host:
                    hostname = host;
                    break;

                case WebRequest request:
                    hostname = request.RequestUri.DnsSafeHost;
                    break;

                default:
                    hostname = string.Empty;
                    break;
            }

            return hostname;
        }

        public class NodeStatus : IDisposable
        {
            private TimeSpan _timerPeriod;
            private readonly RequestExecutor _requestExecutor;
            public readonly int NodeIndex;
            public readonly ServerNode Node;
            private WeakReferencingTimer _timer;

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
                _timer = new WeakReferencingTimer(TimerCallback, this, _timerPeriod, Timeout.InfiniteTimeSpan);
            }

            private static void TimerCallback(object state)
            {
                var nodeStatus = (NodeStatus)state;

                if (nodeStatus._requestExecutor.Disposed)
                {
                    nodeStatus.Dispose();
                    return;
                }
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                nodeStatus._requestExecutor.CheckNodeStatusCallback(nodeStatus);
#pragma warning restore CS4014
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

        public async Task<(int Index, ServerNode Node)> GetRequestedNode(string nodeTag)
        {
            await EnsureNodeSelector().ConfigureAwait(false);
            return _nodeSelector.GetRequestedNode(nodeTag);
        }

        internal async Task<(int Index, ServerNode Node)> GetRequestedNode(string nodeTag, bool throwIfContainsFailures)
        {
            (var index, var node) = await GetRequestedNode(nodeTag).ConfigureAwait(false);
            if (throwIfContainsFailures && _nodeSelector.NodeIsAvailable(index) == false)
            {
                throw new RequestedNodeUnavailableException($"Requested node {nodeTag} currently unavailable, please try again later.");
            }

            return (index, node);
        }

        public async Task<(int Index, ServerNode Node)> GetPreferredNode()
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
            if (_disableTopologyUpdates == false)
                await WaitForTopologyUpdate(_firstTopologyUpdate).ConfigureAwait(false);

            _nodeSelector ??= new NodeSelector(new Topology
            {
                Nodes = TopologyNodes.ToList(),
                Etag = TopologyEtag
            });
        }

        private static void ThrowIfClientException(Exception e)
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

        private class RequestContext
        {
            public bool HttpClientRemoved;
        }

        private class ReturnContext : IDisposable
        {
            private readonly AsyncLocal<RequestContext> _contexts;
            private readonly bool _dispose;

            public ReturnContext(AsyncLocal<RequestContext> contexts, bool dispose)
            {
                _contexts = contexts;
                _dispose = dispose;
            }

            public void Dispose()
            {
                if (_dispose == false)
                    return;

                _contexts.Value = null;
            }
        }

        public class UpdateTopologyParameters
        {
            public ServerNode Node { get; }
            public int TimeoutInMs { get; set; } = 15000;
            public bool ForceUpdate { get; set; }
            public string DebugTag { get; set; }
            public Guid? ApplicationIdentifier { get; set; }

            public UpdateTopologyParameters(ServerNode node)
            {
                Node = node ?? throw new ArgumentNullException(nameof(node));
            }
        }

        private class HttpClientCacheItem
        {
            public HttpClient HttpClient { get; set; }

            public DateTime CreatedAt { get; set; }
        }

        private readonly struct HttpClientCacheKey
        {
            private readonly string _certificateThumbprint;
            private readonly bool _useCompression;
            private readonly TimeSpan? _pooledConnectionLifetime;
            private readonly TimeSpan? _pooledConnectionIdleTimeout;
            private readonly Type _httpClientType;

            public HttpClientCacheKey(string certificateThumbprint, bool useCompression, TimeSpan? pooledConnectionLifetime, TimeSpan? pooledConnectionIdleTimeout, Type httpClientType)
            {
                _certificateThumbprint = certificateThumbprint;
                _useCompression = useCompression;
                _pooledConnectionLifetime = pooledConnectionLifetime;
                _pooledConnectionIdleTimeout = pooledConnectionIdleTimeout;
                _httpClientType = httpClientType;
            }

            private bool Equals(HttpClientCacheKey other)
            {
                return _certificateThumbprint == other._certificateThumbprint
                       && _useCompression == other._useCompression
                       && Nullable.Equals(_pooledConnectionLifetime, other._pooledConnectionLifetime)
                       && Nullable.Equals(_pooledConnectionIdleTimeout, other._pooledConnectionIdleTimeout)
                       && _httpClientType == other._httpClientType;
            }

            public override bool Equals(object obj)
            {
                return obj is HttpClientCacheKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(_certificateThumbprint, _useCompression, _pooledConnectionLifetime, _pooledConnectionIdleTimeout, _httpClientType);
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff(this);
        }

        internal class TestingStuff
        {
            private readonly RequestExecutor _requestExecutor;

            internal TestingStuff(RequestExecutor requestExecutor)
            {
                _requestExecutor = requestExecutor;
            }

            internal int[] NodeSelectorFailures => _requestExecutor._nodeSelector.NodeSelectorFailures;
            internal ConcurrentDictionary<ServerNode, Lazy<NodeStatus>> FailedNodesTimers => _requestExecutor._failedNodesTimers;
            internal (int Index, ServerNode Node) PreferredNode => _requestExecutor._nodeSelector.GetPreferredNode();

            public Action<Task> ExecuteOnAllToFigureOutTheFastestOnTaskCompletion;

            public Action<Task[]> ExecuteOnAllToFigureOutTheFastestOnBeforeWait;

            internal Action<NodeSelector> OnBeforeScheduleSpeedTest;

            internal Action DelayRequest;

            internal Action<GetDatabaseTopologyCommand> SetCommandTimeout;

            internal Task BeforeFetchOperationStatus;
        }
    }
}
