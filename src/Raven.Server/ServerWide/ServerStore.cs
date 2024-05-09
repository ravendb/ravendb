using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using NCrontab.Advanced;
using NCrontab.Advanced.Extensions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Server;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Dashboard;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Commands;
using Raven.Server.Json;
using Raven.Server.Monitoring;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Rachis;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Storage;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;
using Raven.Server.Web.System;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.DiskStatsGetter;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;
using Voron.Exceptions;
using Constants = Raven.Client.Constants;
using DeleteSubscriptionCommand = Raven.Server.ServerWide.Commands.Subscriptions.DeleteSubscriptionCommand;
using MemoryCache = Raven.Server.Utils.Imports.Memory.MemoryCache;
using MemoryCacheOptions = Raven.Server.Utils.Imports.Memory.MemoryCacheOptions;
using NodeInfo = Raven.Client.ServerWide.Commands.NodeInfo;
using Size = Sparrow.Size;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server-wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public partial class ServerStore : IDisposable, ILowMemoryHandler
    {
        private const string ResourceName = nameof(ServerStore);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStore>(ResourceName);

        public const string LicenseStorageKey = "License/Key";

        public const string LicenseLimitsStorageKey = "License/Limits/Key";

        private readonly CancellationTokenSource _shutdownNotification = new CancellationTokenSource();
        private FileLocker _fileLocker;

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        internal StorageEnvironment _env;

        internal readonly SizeLimitedConcurrentDictionary<string, ConcurrentQueue<DateTime>> ClientCreationRate =
            new SizeLimitedConcurrentDictionary<string, ConcurrentQueue<DateTime>>(50);

        private readonly NotificationsStorage _notificationsStorage;
        private readonly OperationsStorage _operationsStorage;
        public ConcurrentDictionary<string, Dictionary<string, long>> IdleDatabases;

        private RequestExecutor _leaderRequestExecutor;
        private long _lastClusterTopologyIndex = -1;

        public readonly RavenConfiguration Configuration;
        private readonly RavenServer _server;
        public readonly DatabasesLandlord DatabasesLandlord;
        public readonly ServerNotificationCenter NotificationCenter;
        public readonly ThreadsInfoNotifications ThreadsInfoNotifications;
        public readonly LicenseManager LicenseManager;
        public readonly FeedbackSender FeedbackSender;
        public readonly StorageSpaceMonitor StorageSpaceMonitor;
        public readonly ServerLimitsMonitor ServerLimitsMonitor;
        public readonly SecretProtection Secrets;
        public readonly AsyncManualResetEvent InitializationCompleted;
        public readonly GlobalIndexingScratchSpaceMonitor GlobalIndexingScratchSpaceMonitor;
        public bool Initialized;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        private Lazy<ClusterRequestExecutor> _clusterRequestExecutor;

        public long LastClientConfigurationIndex { get; private set; } = -2;

        public ConcurrentBackupsCounter ConcurrentBackupsCounter { get; private set; }

        public ServerOperations Operations { get; }

        public CatastrophicFailureNotification CatastrophicFailureNotification { get; }

        public DateTime? LastCertificateUpdateTime { get; private set; }

        internal ClusterRequestExecutor ClusterRequestExecutor => _clusterRequestExecutor.Value;

        private bool IsClusterRequestExecutorCreated => _clusterRequestExecutor.IsValueCreated;

        public ServerStore(RavenConfiguration configuration, RavenServer server)
        {
            // we want our servers to be robust get early errors about such issues
            MemoryInformation.EnableEarlyOutOfMemoryChecks = true;

            DefaultIdentityPartsSeparator = Constants.Identities.DefaultSeparator;

            QueryClauseCache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = configuration.Indexing.QueryClauseCacheSize.GetValue(SizeUnit.Bytes),
                ExpirationScanFrequency = configuration.Indexing.QueryClauseCacheExpirationScanFrequency.AsTimeSpan
            });

            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            FeatureGuardian = new FeatureGuardian(configuration);

            _server = server;

            _clusterRequestExecutor = CreateClusterRequestExecutor();

            IdleDatabases = new ConcurrentDictionary<string, Dictionary<string, long>>(StringComparer.OrdinalIgnoreCase);

            DatabasesLandlord = new DatabasesLandlord(this);

            _notificationsStorage = new NotificationsStorage();

            NotificationCenter = new ServerNotificationCenter(this, _notificationsStorage);

            ThreadsInfoNotifications = new ThreadsInfoNotifications(ServerShutdown);

            _operationsStorage = new OperationsStorage();

            Operations = new ServerOperations(this, _operationsStorage);

            LicenseManager = new LicenseManager(this);

            FeedbackSender = new FeedbackSender();

            StorageSpaceMonitor = new StorageSpaceMonitor(NotificationCenter);

            ServerLimitsMonitor = new ServerLimitsMonitor(this, NotificationCenter, _notificationsStorage);

            DatabaseInfoCache = new DatabaseInfoCache();

            Secrets = new SecretProtection(configuration.Security);

            InitializationCompleted = new AsyncManualResetEvent(_shutdownNotification.Token);

            if (Configuration.Indexing.GlobalScratchSpaceLimit != null)
                GlobalIndexingScratchSpaceMonitor = new GlobalIndexingScratchSpaceMonitor(Configuration.Indexing.GlobalScratchSpaceLimit.Value);

            _frequencyToCheckForIdleDatabases = Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;

            _server.ServerCertificateChanged += OnServerCertificateChanged;

            HasFixedPort = Configuration.Core.ServerUrls == null ||
                           Uri.TryCreate(Configuration.Core.ServerUrls[0], UriKind.Absolute, out var uri) == false ||
                           uri.Port != 0;

            if (Configuration.Indexing.MaxNumberOfConcurrentlyRunningIndexes != null)
                ServerWideConcurrentlyRunningIndexesLock = new FifoSemaphore(Configuration.Indexing.MaxNumberOfConcurrentlyRunningIndexes.Value);

            CatastrophicFailureNotification = new CatastrophicFailureNotification((envId, path, exception, stacktrace) =>
            {
                var message = $"Catastrophic failure in server storage located at '{path}', StackTrace: '{stacktrace}'";

                if (Logger.IsOperationsEnabled)
                {
                    ExecuteSafely(() =>
                    {
                        Logger.OperationsWithWait(message, exception).Wait(TimeSpan.FromSeconds(1));
                    });
                }

                ExecuteSafely(() =>
                {
                    Console.Error.WriteLine($"{message}. Exception: {exception}");
                    Console.Error.Flush();
                });

                Environment.Exit(29); // ERROR_WRITE_FAULT

                static void ExecuteSafely(Action action)
                {
                    try
                    {
                        action();
                    }
                    catch
                    {
                        // nothing we can do
                    }
                }
            });
        }

        private Lazy<ClusterRequestExecutor> CreateClusterRequestExecutor() => new(() => ClusterRequestExecutor.Create(new[] { GetNodeHttpServerUrl() }, Server.Certificate.Certificate, Server.Conventions), LazyThreadSafetyMode.ExecutionAndPublication);

        internal readonly FifoSemaphore ServerWideConcurrentlyRunningIndexesLock;

        private void OnServerCertificateChanged(object sender, EventArgs e)
        {
            Interlocked.Exchange(ref _serverCertificateChanged, 1);

            if (_clusterRequestExecutor.IsValueCreated == false)
                return;

            using (_clusterRequestExecutor.Value)
                _clusterRequestExecutor = CreateClusterRequestExecutor();
        }

        public RavenServer Server => _server;

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public TransactionContextPool ContextPool;

        public IoChangesNotifications IoChanges { get; private set; }

        public long LastRaftCommitIndex
        {
            get
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    return _engine.GetLastCommitIndex(context);
            }
        }

        public ClusterStateMachine Cluster => _engine?.StateMachine;
        public string LeaderTag => _engine.LeaderTag;
        public RachisState CurrentRachisState => _engine.CurrentState;
        public string NodeTag => _engine.Tag;

        public bool Disposed => _disposed;

        private Timer _timer;
        private RachisConsensus<ClusterStateMachine> _engine;
        private bool _disposed;
        public RachisConsensus<ClusterStateMachine> Engine => _engine;

        private ShardingStore _sharding;
        public ShardingStore Sharding => _sharding;


        public ClusterMaintenanceSupervisor ClusterMaintenanceSupervisor;
        private int _serverCertificateChanged;

        private PoolOfThreads.LongRunningWork _clusterMaintenanceSetupTask;
        private PoolOfThreads.LongRunningWork _updateTopologyChangeNotification;

        public bool ValidateFixedPort = true;

        public Dictionary<string, ClusterNodeStatusReport> ClusterStats()
        {
            if (_engine.LeaderTag != NodeTag)
                throw new NotLeadingException($"Stats can be requested only from the raft leader {_engine.LeaderTag}");
            return ClusterMaintenanceSupervisor?.GetStats();
        }

        internal LicenseType GetLicenseType()
        {
            return LicenseManager.LicenseStatus.Type;
        }

        public void UpdateTopologyChangeNotification()
        {
            try
            {
                var delay = 500;
                while (ServerShutdown.IsCancellationRequested == false)
                {
                    Task leaderChangedTask = null;
                    using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerShutdown))
                    {
                        try
                        {
                            _engine.WaitForState(RachisState.Follower, cts.Token).Wait(cts.Token);
                            if (cts.IsCancellationRequested)
                                return;

                            leaderChangedTask = _engine.WaitForLeaderChange(cts.Token);
                            if (Task.WaitAny(new[] { NotificationCenter.WaitForAnyWebSocketClient, leaderChangedTask }, cts.Token) == 1)
                            {
                                // leaderChangedTask has completed
                                continue;
                            }

                            var cancelTask = Task.WhenAny(NotificationCenter.WaitForRemoveAllWebSocketClients, leaderChangedTask);

                            while (cancelTask.IsCompleted == false)
                            {
                                var topology = GetClusterTopology();
                                var leader = _engine.LeaderTag;

                                if (leader == null)
                                {
                                    delay = ReconnectionBackoff(delay);
                                    break;
                                }

                                if (leader == _engine.Tag)
                                    break;

                                var leaderUrl = topology.GetUrlFromTag(leader);
                                if (leaderUrl == null)
                                    break; // will continue from the top of the loop

                                using (var ws = new ClientWebSocket())
                                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                                {
                                    var leaderWsUrl = new Uri($"{leaderUrl.Replace("http", "ws", StringComparison.OrdinalIgnoreCase)}/server/notification-center/watch");

                                    if (Server.Certificate?.Certificate != null)
                                    {
                                        ws.Options.ClientCertificates.Add(Server.Certificate.Certificate);
                                    }

                                    ws.ConnectAsync(leaderWsUrl, cts.Token).Wait(cts.Token);
                                    while (cancelTask.IsCompleted == false &&
                                           (ws.State == WebSocketState.Open || ws.State == WebSocketState.CloseSent))
                                    {
                                        context.Reset();
                                        context.Renew();

                                        var readTask = context.ReadFromWebSocketAsync(ws, "ws from Leader", cts.Token);
                                        using (var notification = readTask.Result)
                                        {
                                            if (notification == null)
                                                break;

                                            if (notification.TryGet(nameof(ClusterTopologyChanged.Type), out NotificationType notificationType) == false ||
                                                notificationType != NotificationType.ClusterTopologyChanged)
                                                continue;

                                            var topologyNotification = JsonDeserializationServer.ClusterTopologyChanged(notification);
                                            if (topologyNotification == null)
                                                continue;

                                            if (_engine.LeaderTag != topologyNotification.Leader)
                                                break;

                                            delay = 500; // on successful read, reset the delay
                                            topologyNotification.NodeTag = _engine.Tag;
                                            topologyNotification.CurrentState = _engine.CurrentState;
                                            NotificationCenter.Add(topologyNotification);
                                        }
                                    }

                                    delay = ReconnectionBackoff(delay);
                                }
                            }
                        }
                        catch (Exception e) when (IsOperationCanceled(e))
                        {
                        }
                        catch (Exception e)
                        {
                            if (Logger.IsInfoEnabled)
                            {
                                Logger.Info($"Error during receiving topology updates from the leader. Waiting {delay} [ms] before trying again.", e);
                            }

                            delay = ReconnectionBackoff(delay);
                        }
                        finally
                        {
                            cts.Cancel();
                            WaitForLeaderChangeTaskToComplete(leaderChangedTask);
                        }
                    }
                }
            }
            catch (Exception e) when (IsOperationCanceled(e))
            {
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations($"Failed to execute {nameof(UpdateTopologyChangeNotification)} task", e);
                }
            }
        }

        private void WaitForLeaderChangeTaskToComplete(Task leaderChangedTask)
        {
            // wait for leader change task to complete
            try
            {
                if (leaderChangedTask == null || leaderChangedTask.IsCompleted)
                    return;

                leaderChangedTask.Wait(ServerShutdown);
            }
            catch (Exception e) when (IsOperationCanceled(e))
            {
                // ignored
            }
        }

        private static bool IsOperationCanceled(Exception e)
        {
            var inner = e.ExtractSingleInnerException();
            return inner is OperationCanceledException;
        }

        private int ReconnectionBackoff(int delay)
        {
            TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(delay), ServerShutdown).Wait(ServerShutdown);
            return Math.Min(15_000, delay * 2);
        }

        internal ClusterObserver Observer { get; set; }

        public void ClusterMaintenanceSetupTask()
        {
            while (ServerShutdown.IsCancellationRequested == false)
            {
                try
                {
                    if (_engine.LeaderTag != NodeTag)
                    {
                        _engine.WaitForState(RachisState.Leader, ServerShutdown)
                            .Wait(ServerShutdown);
                        continue;
                    }

                    var term = _engine.CurrentTerm;
                    using (ClusterMaintenanceSupervisor = new ClusterMaintenanceSupervisor(this, _engine.Tag, term))
                    using (Observer = new ClusterObserver(this, ClusterMaintenanceSupervisor, _engine, term, _engine.ContextPool, ServerShutdown))
                    {
                        var oldNodes = new Dictionary<string, string>();
                        while (_engine.LeaderTag == NodeTag && term == _engine.CurrentTerm)
                        {
                            var topologyChangedTask = _engine.GetTopologyChanged();
                            ClusterTopology clusterTopology;
                            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                            using (context.OpenReadTransaction())
                            {
                                clusterTopology = _engine.GetTopology(context);
                            }

                            var newNodes = clusterTopology.AllNodes;
                            var nodesChanges = ClusterTopology.DictionaryDiff(oldNodes, newNodes);
                            oldNodes = newNodes;

                            foreach (var node in nodesChanges.RemovedValues)
                            {
                                ClusterMaintenanceSupervisor.RemoveFromCluster(node.Key);
                            }

                            foreach (var node in nodesChanges.AddedValues)
                            {
                                ClusterMaintenanceSupervisor.AddToCluster(node.Key, clusterTopology.GetUrlFromTag(node.Key));
                            }

                            var leaderChanged = _engine.WaitForLeaveState(RachisState.Leader, ServerShutdown);

                            if (Task.WaitAny(new[] { topologyChangedTask, leaderChanged }, ServerShutdown) == 1)
                                break;
                        }
                    }
                }
                catch (Exception e) when (IsOperationCanceled(e))
                {
                    return;
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"Failed to execute {nameof(ClusterMaintenanceSetupTask)} task", e);
                    }
                }
            }
        }

        public ClusterTopology GetClusterTopology()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetClusterTopology(context);
            }
        }

        public bool ShouldUpdateTopology(long newRecordIndex, long currentIndex, out string url, ClusterTopology clusterTopology = null)
        {
            if (currentIndex < newRecordIndex)
            {
                clusterTopology ??= GetClusterTopology();
                url = clusterTopology.GetUrlFromTag(NodeTag);
                if (url != null)
                    return true;
            }
            url = null;
            return false;
        }

        public bool HasTopologyChanged(long topologyEtag)
        {
            return _lastClusterTopologyIndex != topologyEtag;
        }

        public ClusterTopology GetClusterTopology<TTransaction>(TransactionOperationContext<TTransaction> context)
            where TTransaction : RavenTransaction
        {
            return _engine.GetTopology(context);
        }

        public bool HasFixedPort { get; internal set; }

        public readonly FeatureGuardian FeatureGuardian;

        public async Task AddNodeToClusterAsync(string nodeUrl, string nodeTag = null, bool validateNotInTopology = true, bool asWatcher = false, CancellationToken token = default)
        {
            if (ValidateFixedPort && HasFixedPort == false)
            {
                throw new InvalidOperationException($"Failed to add node '{nodeUrl}' to cluster. " +
                                                    "Adding nodes to cluster is forbidden when the leader has port '0' in 'Configuration.Core.ServerUrls' setting. " +
                                                    "Define a fixed port for the node to enable cluster creation.");
            }

            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, _shutdownNotification.Token))
                await _engine.AddToClusterAsync(nodeUrl, nodeTag, validateNotInTopology, asWatcher).WithCancellation(cts.Token);
        }

        public async Task RemoveFromClusterAsync(string nodeTag, CancellationToken token = default)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, _shutdownNotification.Token))
                await _engine.RemoveFromClusterAsync(nodeTag).WithCancellation(cts.Token);
        }

        public void RequestSnapshot()
        {
            var topology = GetClusterTopology();

            if (topology.AllNodes.Count == 1)
                throw new InvalidOperationException("Can't force snapshot, since I'm the only node in the cluster.");

            if (topology.Members.ContainsKey(NodeTag))
                throw new InvalidOperationException($"Snapshot can be requested only by a non-member node.{Environment.NewLine}" +
                                                    $"In order to proceed, demote this node to watcher, then request the snapshot again.{Environment.NewLine}" +
                                                    $"Afterwards you can promote it back to member.");

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                _engine.SetSnapshotRequest(ctx, true);
                tx.Commit();
            }
        }

        public void Initialize_Phase_1()
        {
            Configuration.CheckDirectoryPermissions();

            LowMemoryNotification.Instance.Initialize(
                Configuration.Memory.LowMemoryLimit,
                Configuration.Memory.UseTotalDirtyMemInsteadOfMemUsage,
                Configuration.Memory.EnableHighTemporaryDirtyMemoryUse,
                Configuration.Memory.TemporaryDirtyMemoryAllowedPercentage,
                Configuration.Memory.LargeObjectHeapCompactionThresholdPercentage,
                new LowMemoryMonitor(), ServerShutdown);

            MemoryInformation.SetFreeCommittedMemory(
                Configuration.Memory.MinimumFreeCommittedMemoryPercentage,
                Configuration.Memory.MaxFreeCommittedMemoryToKeep,
                Configuration.Memory.LowMemoryCommitLimit);

            if (Logger.IsInfoEnabled)
                Logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

            var path = Configuration.Core.DataDirectory.Combine("System");
            var storeAlertForLateRaise = new List<AlertRaised>();

            IoChanges = new IoChangesNotifications
            {
                DisableIoMetrics = Configuration.Storage.EnableIoMetrics == false
            };

            StorageEnvironmentOptions options;
            if (Configuration.Core.RunInMemory)
            {
                options = StorageEnvironmentOptions.CreateMemoryOnly(null, null, null, CatastrophicFailureNotification);
            }
            else
            {
                _fileLocker = new FileLocker(Path.Combine(path.FullPath, "system.lock"));
                _fileLocker.TryAcquireWriteLock(Logger);

                string tempPath = null;

                if (Configuration.Storage.TempPath != null)
                    tempPath = Configuration.Storage.TempPath.Combine("System").FullPath;

                options = StorageEnvironmentOptions.ForPath(path.FullPath, tempPath, null, IoChanges, CatastrophicFailureNotification);
                var secretKey = Path.Combine(path.FullPath, "secret.key.encrypted");
                if (File.Exists(secretKey))
                {
                    byte[] buffer;
                    try
                    {
                        buffer = File.ReadAllBytes(secretKey);
                    }
                    catch (Exception e)
                    {
                        throw new FileLoadException($"The server store secret key is provided in {secretKey} but the server failed to read the file. Admin assistance required.", e);
                    }

                    options.DoNotConsiderMemoryLockFailureAsCatastrophicError = Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
                    try
                    {
                        options.Encryption.MasterKey = Secrets.Unprotect(buffer);
                    }
                    catch (Exception e)
                    {
                        throw new CryptographicException($"Unable to unprotect the secret key file {secretKey}. " +
                                                         "Was the server store encrypted using a different OS user? In that case, " +
                                                         "you must provide an unprotected key (rvn offline-operation put-key). " +
                                                         "Admin assistance required.", e);
                    }
                }
            }

            options.OnNonDurableFileSystemError += (obj, e) =>
            {
                var title = "Non Durable File System - System Storage";

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"{title}. {e.Message}", e.Exception);

                var alert = AlertRaised.Create(
                    null,
                    title,
                    e.Message,
                    AlertType.NonDurableFileSystem,
                    NotificationSeverity.Warning,
                    "NonDurable Error System",
                    details: new MessageDetails { Message = e.Details });
                if (NotificationCenter.IsInitialized)
                {
                    NotificationCenter.Add(alert);
                }
                else
                {
                    storeAlertForLateRaise.Add(alert);
                }
            };

            options.OnRecoveryError += (obj, e) =>
            {
                string title = "Recovery Error - System Storage";

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"{title}. {e.Message}", e.Exception);

                var alert = AlertRaised.Create(
                    null,
                    title,
                    e.Message,
                    AlertType.RecoveryError,
                    NotificationSeverity.Error,
                    key: $"Recovery Error System/{SystemTime.UtcNow.Ticks % 5}"); // if this was called multiple times let's try to not overwrite previous alerts

                if (NotificationCenter.IsInitialized)
                {
                    NotificationCenter.Add(alert);
                }
                else
                {
                    storeAlertForLateRaise.Add(alert);
                }
            };

            options.OnIntegrityErrorOfAlreadySyncedData += (obj, e) =>
            {
                string title = "Integrity error of already synced data - System Storage";

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"{title}. {e.Message}", e.Exception);

                var alert = AlertRaised.Create(
                    null,
                    title,
                    e.Message,
                    AlertType.IntegrityErrorOfAlreadySyncedData,
                    NotificationSeverity.Warning,
                    key: $"Integrity Error of Synced Data - System/{SystemTime.UtcNow.Ticks % 5}"); // if this was called multiple times let's try to not overwrite previous alerts

                if (NotificationCenter.IsInitialized)
                {
                    NotificationCenter.Add(alert);
                }
                else
                {
                    storeAlertForLateRaise.Add(alert);
                }
            };

            try
            {
                var swapping = PlatformSpecific.MemoryInformation.IsSwappingOnHddInsteadOfSsd();
                if (swapping != null)
                {
                    var alert = AlertRaised.Create(
                        null,
                        "Swap Storage Type Warning",
                        "OS swapping on at least one HDD drive while there is at least one SSD drive on this system. " +
                        "This can cause a slowdown, consider moving swap-partition/pagefile to SSD. The current HDD spinning drive with swapping : " + swapping,
                        AlertType.SwappingHddInsteadOfSsd,
                        NotificationSeverity.Warning);
                    if (NotificationCenter.IsInitialized)
                    {
                        NotificationCenter.Add(alert);
                    }
                    else
                    {
                        storeAlertForLateRaise.Add(alert);
                    }
                }
            }
            catch (Exception e)
            {
                // the above should not throw, but we mask it in case it does (as it reads IO parameters) - this alert is just a nice-to-have warning
                if (Logger.IsInfoEnabled)
                    Logger.Info("An error occurred while trying to determine Is Swapping On Hdd Instead Of Ssd", e);
            }

            options.SchemaVersion = SchemaUpgrader.CurrentVersion.ServerVersion;
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Server, null, null, this);
            options.BeforeSchemaUpgrade = _server.BeforeSchemaUpgrade;
            options.ForceUsing32BitsPager = Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = Configuration.Storage.EnablePrefetching;
            options.DiscardVirtualMemory = Configuration.Storage.DiscardVirtualMemory;

            if (Configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = Configuration.Storage.SyncJournalsCountThreshold;
            options.IgnoreInvalidJournalErrors = Configuration.Storage.IgnoreInvalidJournalErrors;
            options.SkipChecksumValidationOnDatabaseLoading = Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = Configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;

            DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, Configuration.Storage, nameof(DirectoryExecUtils.EnvironmentType.System), DirectoryExecUtils.EnvironmentType.System, Logger);

            try
            {
                StorageEnvironment.MaxConcurrentFlushes = Configuration.Storage.MaxConcurrentFlushes;

                try
                {
                    _env = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.System);
                }
                catch (Exception e)
                {
                    throw new ServerLoadFailureException("Failed to load system storage " + Environment.NewLine + $"At {options.BasePath}", e);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations(
                        "Could not open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath), e);
                options.Dispose();
                throw;
            }

            if (Configuration.Queries.MaxClauseCount != null)
                BooleanQuery.MaxClauseCount = Configuration.Queries.MaxClauseCount.Value;

            ContextPool = new TransactionContextPool(_env, Configuration.Memory.MaxContextSizeToKeep);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                // warm-up the json convertor, it takes about 250ms at first conversion.
                DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(new DatabaseRecord(), ctx);
            }

            _server.Statistics.Load(ContextPool, Logger);

            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            _notificationsStorage.Initialize(_env, ContextPool);
            _operationsStorage.Initialize(_env, ContextPool);
            DatabaseInfoCache.Initialize(_env, ContextPool);

            NotificationCenter.Initialize();
            foreach (var alertRaised in storeAlertForLateRaise)
            {
                NotificationCenter.Add(alertRaised);
            }

            CheckSwapOrPageFileAndRaiseNotification();
        }

        public void Initialize_Phase_2()
        {
            var clusterChanges = new ClusterChanges();
            _sharding = new ShardingStore(this);
            _engine = new RachisConsensus<ClusterStateMachine>(this);

            var myUrl = GetNodeHttpServerUrl();
            _engine.Initialize(_env, Configuration, clusterChanges, myUrl, Server.Time, out _lastClusterTopologyIndex, ServerShutdown);

            using (Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                PublishedServerUrls = PublishedServerUrls.Read(context);

                LoadDefaultIdentityPartsSeparator(context);
            }

            _ = Task.Run(PublishServerUrlAsync).IgnoreUnobservedExceptions();

            SorterCompilationCache.Instance.AddServerWideItems(this);
            AnalyzerCompilationCache.Instance.AddServerWideItems(this);

            LicenseManager.Initialize(_env, ContextPool);
            LatestVersionCheck.Instance.Check(this);

            ConcurrentBackupsCounter = new ConcurrentBackupsCounter(Configuration.Backup, LicenseManager);

            ConfigureAuditLog();

            Initialized = true;
            InitializationCompleted.Set();
        }

        public void LoadDefaultIdentityPartsSeparator(ClientConfiguration clientConfiguration)
        {
            var defaultIdentityPartsSeparator = Constants.Identities.DefaultSeparator;
            if (clientConfiguration is { Disabled: false, IdentityPartsSeparator: not null })
                defaultIdentityPartsSeparator = clientConfiguration.IdentityPartsSeparator.Value;

            DefaultIdentityPartsSeparator = defaultIdentityPartsSeparator;
        }

        private void LoadDefaultIdentityPartsSeparator(ClusterOperationContext context)
        {
            ClientConfiguration clientConfiguration = null;
            var serverClientConfigurationJson = Cluster.Read(context, Constants.Configuration.ClientId, out _);
            if (serverClientConfigurationJson != null)
                clientConfiguration = JsonDeserializationClient.ClientConfiguration(serverClientConfigurationJson);

            LoadDefaultIdentityPartsSeparator(clientConfiguration);
        }

        private async Task PublishServerUrlAsync()
        {
            string publicUrl = null;
            string privateUrl = null;

            while (ServerShutdown.IsCancellationRequested == false)
            {
                try
                {
                    if (IsPassive())
                        await Engine.WaitForLeaveState(RachisState.Passive, ServerShutdown);

                    if (Engine.CommandsVersionManager.CurrentClusterMinimalVersion < 60_000)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(15), ServerShutdown);
                        continue;
                    }

                    publicUrl ??= GetNodeHttpServerUrl();
                    privateUrl ??= Configuration.Core.ClusterServerUrl?.ToString() ?? publicUrl;

                    var cmd = new UpdateServerPublishedUrlsCommand(NodeTag, publicUrl, privateUrl, Guid.NewGuid().ToString());
                    await SendToLeaderAsync(cmd);
                    return;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to update my private url to {privateUrl ?? "N/A"}", e);

                    await Task.Delay(TimeSpan.FromSeconds(1), ServerShutdown);
                }
            }
        }

        private void CheckSwapOrPageFileAndRaiseNotification()
        {
            if (PlatformDetails.RunningOnLinux)
            {
                if (PlatformDetails.RunningOnDocker)
                    return;

                var errorThreshold = new Sparrow.Size(128, SizeUnit.Megabytes);
                var swapSize = MemoryInformation.GetMemoryInfo().TotalSwapSize;
                if (swapSize < Configuration.PerformanceHints.MinSwapSize - errorThreshold)
                {
                    bool noSwapFile = swapSize == Size.Zero;
                    string title = noSwapFile ? "No swap file" : "Low swap size";
                    string message = noSwapFile ? $"There is no swap file, it is advised to set up a '{Configuration.PerformanceHints.MinSwapSize}' swap file" :
                        $"The current swap size is '{swapSize}' and it is lower then the threshold defined '{Configuration.PerformanceHints.MinSwapSize}'";

                    NotificationCenter.Add(AlertRaised.Create(null,
                        title,
                        message,
                        AlertType.LowSwapSize,
                        NotificationSeverity.Warning));
                }

                return;
            }

            if (PlatformDetails.RunningOnPosix == false)
            {
                var memoryInfo = MemoryInformation.GetMemoryInfo();
                if (memoryInfo.TotalCommittableMemory - memoryInfo.TotalPhysicalMemory <= Sparrow.Size.Zero)
                    NotificationCenter.Add(AlertRaised.Create(null,
                        "No PageFile available",
                        "Your system has no PageFile. It is recommended to have a PageFile in order for Server to work properly",
                        AlertType.LowSwapSize,
                        NotificationSeverity.Warning));
            }
        }

        private void ConfigureAuditLog()
        {
            if (Configuration.Security.AuditLogPath == null)
                return;

            if (Configuration.Security.AuthenticationEnabled == false)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("The audit log configuration 'Security.AuditLog.FolderPath' was specified, but the server is not running in a secured mode. Audit log disabled!");
                return;
            }

            // we have to do this manually because LoggingSource will ignore errors
            AssertCanWriteToAuditLogDirectory();

            LoggingSource.AuditLog.MaxFileSizeInBytes = Configuration.Logs.MaxFileSize.GetValue(SizeUnit.Bytes);
            LoggingSource.AuditLog.SetupLogMode(
                LogMode.Information,
                Configuration.Security.AuditLogPath.FullPath,
                Configuration.Security.AuditLogRetentionTime.AsTimeSpan,
                Configuration.Security.AuditLogRetentionSize?.GetValue(SizeUnit.Bytes),
                Configuration.Security.AuditLogCompress);

            var auditLog = LoggingSource.AuditLog.GetLogger("ServerStartup", "Audit");
            auditLog.Operations($"Server started up, listening to {string.Join(", ", Configuration.Core.ServerUrls)} with certificate {_server.Certificate?.Certificate?.Subject} ({_server.Certificate?.Certificate?.Thumbprint}), public url: {Configuration.Core.PublicServerUrl}");
        }

        private void AssertCanWriteToAuditLogDirectory()
        {
            if (Directory.Exists(Configuration.Security.AuditLogPath.FullPath) == false)
            {
                try
                {
                    Directory.CreateDirectory(Configuration.Security.AuditLogPath.FullPath);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Cannot create audit log directory: {Configuration.Security.AuditLogPath.FullPath}, treating this as a fatal error", e);
                }
            }
            try
            {
                var testFile = Configuration.Security.AuditLogPath.Combine("write.test").FullPath;
                File.WriteAllText(testFile, "test we can write");
                File.Delete(testFile);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot create new file in audit log directory: {Configuration.Security.AuditLogPath.FullPath}, treating this as a fatal error", e);
            }
        }


        public void TriggerDatabases()
        {
            _engine.StateMachine.Changes.DatabaseChanged += DatabasesLandlord.ClusterOnDatabaseChanged;
            _engine.StateMachine.Changes.DatabaseChanged += OnDatabaseChanged;
            _engine.StateMachine.Changes.ValueChanged += OnValueChanged;

            _engine.TopologyChanged += (_, __) => NotifyAboutClusterTopologyAndConnectivityChanges();
            _engine.StateChanged += OnStateChanged;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var db in _engine.StateMachine.GetDatabaseNames(context))
                {
                    Task.Run(async () =>
                        {
                            try
                            {
                                await DatabasesLandlord.ClusterOnDatabaseChanged(db, 0, DatabasesLandlord.Init, DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged, null);
                            }
                            catch (Exception e)
                            {
                                if (ServerShutdown.IsCancellationRequested)
                                    return;

                                if (Logger.IsInfoEnabled)
                                {
                                    Logger.Info($"Failed to trigger database {db}.", e);
                                }
                            }
                        },
                        ServerShutdown);
                }

                if (_engine.StateMachine.Read(context, Constants.Configuration.ClientId, out long clientConfigEtag) != null)
                    LastClientConfigurationIndex = clientConfigEtag;
            }

            _clusterMaintenanceSetupTask = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
                ClusterMaintenanceSetupTask(), null, ThreadNames.ForClusterMaintenanceSetupTask("Cluster Maintenance Setup Task"));

            const string threadName = "Update Topology Change Notification Task";
            _updateTopologyChangeNotification = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                UpdateTopologyChangeNotification();
            }, null, ThreadNames.ForUpdateTopologyChangeNotificationTask(threadName));
        }

        private void OnStateChanged(object sender, RachisConsensus.StateTransition state)
        {
            var msg = $"{DateTime.UtcNow}, State changed: {state.From} -> {state.To} in term {state.CurrentTerm}, because {state.Reason}";

            if (Engine.Log.IsInfoEnabled)
            {
                Engine.Log.Info(msg);
            }
            Engine.InMemoryDebug.StateChangeTracking.LimitedSizeEnqueue(msg, 10);

            NotifyAboutClusterTopologyAndConnectivityChanges();

            // if we are in passive/candidate state, we prevent from tasks to be performed by this node.
            if (state.From == RachisState.Passive || state.To == RachisState.Passive ||
                state.From == RachisState.Candidate || state.To == RachisState.Candidate)
            {
                ThreadPool.QueueUserWorkItem(async _ =>
                {
                    await RefreshOutgoingTasksAsync();
                }, null);
            }
        }

        private async Task RefreshOutgoingTasksAsync()
        {
            var tasks = new Dictionary<string, Task<DocumentDatabase>>();
            foreach (var db in DatabasesLandlord.DatabasesCache)
            {
                tasks.Add(db.Key.Value, db.Value);
            }
            while (tasks.Count != 0)
            {
                var completedTask = await Task.WhenAny(tasks.Values).ConfigureAwait(false);
                var name = tasks.Single(t => t.Value == completedTask).Key;
                tasks.Remove(name);
                try
                {
                    var database = await completedTask.ConfigureAwait(false);
                    await database.RefreshFeaturesAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // database shutdown
                }
                catch (ObjectDisposedException)
                {
                    // database shutdown
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"An error occurred while disabling outgoing tasks on the database {name}", e);
                    }
                }
            }
        }

        public Dictionary<string, NodeStatus> GetNodesStatuses()
        {
            Dictionary<string, NodeStatus> nodesStatuses = null;

            switch (CurrentRachisState)
            {
                case RachisState.Leader:
                    nodesStatuses = _engine.CurrentLeader?.GetStatus();

                    break;

                case RachisState.Candidate:
                    nodesStatuses = _engine.Candidate?.GetStatus();

                    break;

                case RachisState.Follower:
                    var leaderTag = _engine.LeaderTag;
                    if (leaderTag != null)
                    {
                        nodesStatuses = new Dictionary<string, NodeStatus>
                        {
                            [leaderTag] = new NodeStatus { Connected = true }
                        };
                    }
                    break;
            }

            return nodesStatuses ?? new Dictionary<string, NodeStatus>();
        }

        private readonly MultipleUseFlag _notify = new MultipleUseFlag();

        public void NotifyAboutClusterTopologyAndConnectivityChanges()
        {
            if (_notify.Raise() == false)
                return;

            Task.Run(async () =>
            {
                while (_notify.Lower())
                {
                    try
                    {
                        if (ServerShutdown.IsCancellationRequested)
                            return;

                        var clusterTopology = GetClusterTopology();

                        if (_engine.CurrentState != RachisState.Follower)
                        {
                            OnTopologyChangeInternal(clusterTopology, leaderClusterTopology: null, new ServerNode() { ClusterTag = NodeTag, Url = GetNodeHttpServerUrl() });
                            return;
                        }

                        // need to get it from the leader
                        var leaderTag = LeaderTag;
                        if (leaderTag == null)
                            return;

                        var leaderUrl = clusterTopology.GetUrlFromTag(leaderTag);
                        if (leaderUrl == null)
                            return;

                        using (var clusterRequestExecutor = CreateNewClusterRequestExecutor(leaderUrl))
                        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                        {
                            var command = new GetClusterTopologyCommand(NodeTag);
                            await clusterRequestExecutor.ExecuteAsync(command, context, token: ServerShutdown);
                            var response = command.Result;

                            OnTopologyChangeInternal(clusterTopology, response.Topology, new ServerNode() { ClusterTag = leaderTag, Url = leaderUrl }, response.Status);
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        // shutdown
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info("Unable to notify about cluster topology change", e);
                        }
                    }
                }
            }, ServerShutdown);
        }

        private void OnTopologyChangeInternal(ClusterTopology localClusterTopology, ClusterTopology leaderClusterTopology, ServerNode topologyNode, Dictionary<string, NodeStatus> status = null)
        {
            var topology = leaderClusterTopology ?? localClusterTopology;

            //ClusterTopologyChanged notification is used by studio to show the connectivity state of the node
            //so we want to always fire it, regardless of the topology etag
            NotificationCenter.Add(ClusterTopologyChanged.Create(topology, LeaderTag, NodeTag, _engine.CurrentTerm, _engine.CurrentState,
                status ?? GetNodesStatuses(), LoadLicenseLimits()?.NodeLicenseDetails));

            if (ShouldUpdateTopology(topology.Etag, _lastClusterTopologyIndex, out _, localClusterTopology))
            {
                if (IsClusterRequestExecutorCreated)
                {
                    _ = ClusterRequestExecutor.UpdateTopologyAsync(
                        new RequestExecutor.UpdateTopologyParameters(topologyNode)
                        {
                            DebugTag = "cluster-topology-update"
                        });
                }

                _lastClusterTopologyIndex = topology.Etag;
            }
        }

        private Task OnDatabaseChanged(string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType _, object state)
        {
            switch (type)
            {
                case nameof(DeleteDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(databaseName, DatabaseChangeType.Delete));
                    break;

                case nameof(AddDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(databaseName, DatabaseChangeType.Put));
                    break;

                case nameof(ToggleDatabasesStateCommand):
                case nameof(UpdateTopologyCommand):
                case nameof(EditLockModeCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(databaseName, DatabaseChangeType.Update));
                    break;

                case nameof(RemoveNodeFromDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(databaseName, DatabaseChangeType.RemoveNode));
                    break;
                case nameof(PutServerWideBackupConfigurationCommand):
                    RescheduleTimerIfDatabaseIdle(databaseName, state);
                    break;
            }

            return Task.CompletedTask;
        }

        private async Task OnValueChanged(long index, string type)
        {
            switch (type)
            {
                case nameof(RecheckStatusOfServerCertificateCommand):
                case nameof(ConfirmReceiptServerCertificateCommand):
                    await ConfirmCertificateReceiptValueChanged(index, type);
                    break;

                case nameof(InstallUpdatedServerCertificateCommand):
                    await InstallUpdatedCertificateValueChanged(index, type);
                    break;

                case nameof(RecheckStatusOfServerCertificateReplacementCommand):
                case nameof(ConfirmServerCertificateReplacedCommand):
                    ConfirmCertificateReplacedValueChanged(index, type);
                    break;

                case nameof(PutClientConfigurationCommand):
                    LastClientConfigurationIndex = index;
                    break;

                case nameof(PutLicenseCommand):

                    ForTestingPurposes?.BeforePutLicenseCommandHandledInOnValueChanged?.Invoke();

                    // reload license can send a notification which will open a write tx
                    LicenseManager.ReloadLicense();
                    ConcurrentBackupsCounter.ModifyMaxConcurrentBackups();

                    // we are not waiting here on purpose
                    _ = LicenseManager.PutMyNodeInfoAsync().IgnoreUnobservedExceptions();
                    break;

                case nameof(PutLicenseLimitsCommand):
                case nameof(UpdateLicenseLimitsCommand):
                    LicenseManager.ReloadLicenseLimits();
                    ConcurrentBackupsCounter.ModifyMaxConcurrentBackups();
                    NotifyAboutClusterTopologyAndConnectivityChanges();
                    break;
                case nameof(PutCertificateCommand):
                    LastCertificateUpdateTime = SystemTime.UtcNow;
                    break;

                case nameof(UpdateServerPublishedUrlsCommand):
                    using (Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        PublishedServerUrls = PublishedServerUrls.Read(context);
                    }

                    foreach (var orchestrator in DatabasesLandlord.ShardedDatabasesCache.Values)
                    {
                        if (orchestrator.IsCompletedSuccessfully == false)
                            continue;

                        await orchestrator.Result.UpdateUrlsAsync(index);
                    }

                    break;
            }
        }

        public PublishedServerUrls PublishedServerUrls;

        private void RescheduleTimerIfDatabaseIdle(string db, object state)
        {
            if (IdleDatabases.ContainsKey(db) == false)
                return;

            if (state is long taskId == false)
            {
                Debug.Assert(state == null,
                    $"This is probably a bug. This method should be called only for {nameof(PutServerWideBackupConfigurationCommand)} and the state should be the database periodic backup task id.");
                //The database is excluded from the server-wide backup.
                return;
            }

            PeriodicBackupConfiguration backupConfig;
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, db))
            {
                backupConfig = rawRecord.GetPeriodicBackupConfiguration(taskId);

                if (backupConfig == null)
                {
                    //`indexPerDatabase` was collected from the previous transaction. The database can be excluded in the meantime. 
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Could not reschedule the wakeup timer for idle database '{db}', because there is no backup task with id '{taskId}'.");
                    return;
                }
            }

            var tag = BackupUtils.GetResponsibleNodeTag(Server.ServerStore, db, backupConfig.TaskId);
            if (Engine.Tag != tag)
            {
                if (Logger.IsOperationsEnabled && tag != null)
                    Logger.Operations($"Could not reschedule the wakeup timer for idle database '{db}', because backup task '{backupConfig.Name}' with id '{taskId}' belongs to node '{tag}' current node is '{Engine.Tag}'.");
                return;
            }

            if (backupConfig.Disabled || backupConfig.FullBackupFrequency == null && backupConfig.IncrementalBackupFrequency == null)
                return;

            var now = SystemTime.UtcNow;
            DateTime wakeup;
            if (backupConfig.FullBackupFrequency == null)
            {
                wakeup = CrontabSchedule.Parse(backupConfig.IncrementalBackupFrequency).GetNextOccurrence(now);
            }
            else
            {
                wakeup = CrontabSchedule.Parse(backupConfig.FullBackupFrequency).GetNextOccurrence(now);
                if (backupConfig.IncrementalBackupFrequency != null)
                {
                    var incremental = CrontabSchedule.Parse(backupConfig.IncrementalBackupFrequency).GetNextOccurrence(now);
                    wakeup = new DateTime(Math.Min(wakeup.Ticks, incremental.Ticks));
                }
            }

            wakeup = DateTime.SpecifyKind(wakeup, DateTimeKind.Utc);
            var nextIdleDatabaseActivity = new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, wakeup);
            DatabasesLandlord.RescheduleNextIdleDatabaseActivity(db, nextIdleDatabaseActivity);

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Rescheduling the wakeup timer for idle database '{db}', because backup task '{backupConfig.Name}' with id '{taskId}' which belongs to node '{Engine.Tag}', new timer is set to: '{nextIdleDatabaseActivity.DateTime}', with dueTime: {nextIdleDatabaseActivity.DueTime} ms.");

        }

        private void ConfirmCertificateReplacedValueChanged(long index, string type)
        {
            try
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    int nodesInCluster;
                    int replaced;
                    string thumbprint;
                    string oldThumbprint;

                    using (context.OpenReadTransaction())
                    {
                        var cert = Cluster.GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                        if (cert == null)
                            return;

                        if (cert.TryGet(nameof(CertificateReplacement.Thumbprint), out thumbprint) == false)
                            throw new InvalidOperationException($"Expected to get `{nameof(CertificateReplacement.Thumbprint)}` property");

                        if (cert.TryGet(nameof(CertificateReplacement.Replaced), out replaced) == false)
                            throw new InvalidOperationException($"Expected to get '{nameof(CertificateReplacement.Replaced)}' count");

                        if (cert.TryGet(nameof(CertificateReplacement.OldThumbprint), out oldThumbprint) == false)
                            throw new InvalidOperationException($"Expected to get `{nameof(CertificateReplacement.OldThumbprint)}` property");

                        nodesInCluster = GetClusterTopology(context).AllNodes.Count;
                    }

                    if (thumbprint == Server.Certificate?.Certificate?.Thumbprint)
                    {
                        if (nodesInCluster > replaced)
                        {
                            // I already replaced it, but not all nodes did
                            if (Logger.IsOperationsEnabled)
                                Logger.Operations($"The server certificate was successfully replaced in {replaced} nodes out of {nodesInCluster}.");

                            return;
                        }

                        // I replaced it as did everyone else, we can safely delete the "server/cert" doc
                        // as well as the old and new server certs from the server store trusted certificates
                        using (var tx = context.OpenWriteTransaction())
                        {
                            ClusterStateMachine.DeleteItem(context, CertificateReplacement.CertificateReplacementDoc);
                            Cluster.DeleteCertificate(context, thumbprint);

                            if (oldThumbprint.IsNullOrWhiteSpace() == false)
                                Cluster.DeleteCertificate(context, oldThumbprint);

                            tx.Commit();
                        }

                        if (Logger.IsOperationsEnabled)
                            Logger.Operations("The server certificate was successfully replaced in the entire cluster.");

                        NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Certificates_ReplaceSuccess, null));
                        NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Certificates_ReplaceError, null));
                        NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Certificates_ReplacePending, null));

                        NotificationCenter.Add(AlertRaised.Create(
                            null,
                            CertificateReplacement.CertReplaceAlertTitle,
                            "The server certificate was successfully replaced in the entire cluster.",
                            AlertType.Certificates_EntireClusterReplaceSuccess,
                            NotificationSeverity.Success));
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to process {type}.", e);

                NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    $"Failed to process {type}.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        private async Task InstallUpdatedCertificateValueChanged(long index, string type)
        {
            try
            {
                string certThumbprint;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cert = Cluster.GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                    if (cert == null)
                        return;
                    if (cert.TryGet(nameof(CertificateReplacement.Thumbprint), out certThumbprint) == false)
                        throw new InvalidOperationException($"Invalid 'server/cert' value, expected to get '{nameof(CertificateReplacement.Thumbprint)}' property");

                    if (cert.TryGet(nameof(CertificateReplacement.Certificate), out string base64Cert) == false)
                        throw new InvalidOperationException($"Invalid 'server/cert' value, expected to get '{nameof(CertificateReplacement.Certificate)}' property");

                    var certificate = CertificateLoaderUtil.CreateCertificate(Convert.FromBase64String(base64Cert));

                    var now = Server.Time.GetUtcNow();
                    if (certificate.NotBefore.ToUniversalTime() > now)
                    {
                        var msg = "Unable to confirm certificate replacement because the NotBefore property is set " +
                                  $"to {certificate.NotBefore.ToUniversalTime():O} and now it is {now:O}. Will try again later";

                        if (Logger.IsOperationsEnabled)
                            Logger.Operations(msg);

                        NotificationCenter.Add(AlertRaised.Create(
                            null,
                            CertificateReplacement.CertReplaceAlertTitle,
                            msg,
                            AlertType.Certificates_ReplaceError,
                            NotificationSeverity.Error));
                        return;
                    }
                }

                // we got it, now let us let the leader know about it
                await SendToLeaderAsync(new ConfirmReceiptServerCertificateCommand(certThumbprint));
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to process {type}.", e);

                NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    $"Failed to process {type}.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        private async Task ConfirmCertificateReceiptValueChanged(long index, string type)
        {
            try
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    string certBase64;
                    string oldThumbprint;

                    using (context.OpenReadTransaction())
                    {
                        var cert = Cluster.GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                        if (cert == null)
                            return;
                        if (cert.TryGet(nameof(CertificateReplacement.Confirmations), out int confirmations) == false)
                            throw new InvalidOperationException($"Expected to get '{nameof(CertificateReplacement.Confirmations)}' count");

                        if (cert.TryGet(nameof(CertificateReplacement.ReplaceImmediately), out bool replaceImmediately) == false)
                            throw new InvalidOperationException($"Expected to get `{nameof(CertificateReplacement.ReplaceImmediately)}` property");

                        int nodesInCluster = GetClusterTopology(context).AllNodes.Count;

                        if (nodesInCluster > confirmations && replaceImmediately == false)
                        {
                            if (Server.Certificate?.Certificate?.NotAfter != null &&
                                (Server.Certificate.Certificate.NotAfter - Server.Time.GetUtcNow().ToLocalTime()).Days > 3)
                            {
                                var msg = $"Not all nodes have confirmed the certificate replacement. Confirmation count: {confirmations}. " +
                                          $"We still have {(Server.Certificate.Certificate.NotAfter - Server.Time.GetUtcNow().ToLocalTime()).Days} days until expiration. " +
                                          "The update will happen when all nodes confirm the replacement or we have less than 3 days left for expiration." +
                                          $"If you wish to force replacing the certificate just for the nodes that are up, please set '{nameof(CertificateReplacement.ReplaceImmediately)}' to true.";

                                if (Logger.IsOperationsEnabled)
                                    Logger.Operations(msg);

                                NotificationCenter.Add(AlertRaised.Create(
                                    null,
                                    CertificateReplacement.CertReplaceAlertTitle,
                                    msg,
                                    AlertType.Certificates_ReplacePending,
                                    NotificationSeverity.Warning));
                                return;
                            }
                        }

                        if (cert.TryGet(nameof(CertificateReplacement.Certificate), out certBase64) == false ||
                            cert.TryGet(nameof(CertificateReplacement.Thumbprint), out string certThumbprint) == false)
                            throw new InvalidOperationException(
                                $"Invalid 'server/cert' value, expected to get '{nameof(CertificateReplacement.Certificate)}' and '{nameof(CertificateReplacement.Thumbprint)}' properties");

                        if (certThumbprint == Server.Certificate?.Certificate?.Thumbprint)
                            return;

                        if (cert.TryGet(nameof(CertificateReplacement.OldThumbprint), out oldThumbprint) == false)
                            oldThumbprint = string.Empty;
                    }

                    // Save the received certificate

                    var bytesToSave = Convert.FromBase64String(certBase64);
                    var newClusterCertificate = CertificateLoaderUtil.CreateCertificate(bytesToSave, flags: CertificateLoaderUtil.FlagsForExport);

                    if (string.IsNullOrEmpty(Configuration.Security.CertificatePath) == false)
                    {
                        if (string.IsNullOrEmpty(Configuration.Security.CertificatePassword) == false)
                        {
                            bytesToSave = newClusterCertificate.Export(X509ContentType.Pkcs12, Configuration.Security.CertificatePassword);
                        }

                        var certPath = Path.Combine(AppContext.BaseDirectory, Configuration.Security.CertificatePath);
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations($"Writing the new certificate to {certPath}");

                        try
                        {
                            await using (var certStream = File.Create(certPath))
                            {
                                await certStream.WriteAsync(bytesToSave, 0, bytesToSave.Length, ServerShutdown);
                                await certStream.FlushAsync(ServerShutdown);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new IOException($"Cannot write certificate to {certPath} , RavenDB needs write permissions for this file.", e);
                        }
                    }
                    else if (string.IsNullOrEmpty(Configuration.Security.CertificateChangeExec) == false)
                    {
                        try
                        {
                            Secrets.NotifyExecutableOfCertificateChange(Configuration.Security.CertificateChangeExec, Configuration.Security.CertificateChangeExecArguments, certBase64);
                        }
                        catch (Exception e)
                        {
                            if (Logger.IsOperationsEnabled)
                                Logger.Operations($"Unable to notify executable about the cluster certificate change '{Server.Certificate.Certificate.Thumbprint}'.", e);
                        }
                    }
                    else
                    {
                        var msg = "Cluster wanted to install updated server certificate, but no path or executable has been configured in settings.json";
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations(msg);

                        NotificationCenter.Add(AlertRaised.Create(
                            null,
                            CertificateReplacement.CertReplaceAlertTitle,
                            msg,
                            AlertType.Certificates_ReplaceError,
                            NotificationSeverity.Error));
                        return;
                    }

                    // and now we have to replace the cert in the running server...

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Replacing the certificate used by the server to: {newClusterCertificate.Thumbprint} ({newClusterCertificate.SubjectName.Name})");

                    Server.SetCertificate(newClusterCertificate, bytesToSave, Configuration.Security.CertificatePassword);

                    NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Certificates_ReplaceError, null));
                    NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Certificates_ReplacePending, null));

                    NotificationCenter.Add(AlertRaised.Create(
                        null,
                        CertificateReplacement.CertReplaceAlertTitle,
                        $"The server certificate was successfully replaced on node {NodeTag}.",
                        AlertType.Certificates_ReplaceSuccess,
                        NotificationSeverity.Success));

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"The server certificate was successfully replaced on node {NodeTag}.");

                    if (ClusterCommandsVersionManager.ClusterCommandsVersions.TryGetValue(nameof(ConfirmServerCertificateReplacedCommand), out var commandVersion) == false)
                        throw new InvalidOperationException($"Failed to get the command version of '{nameof(ConfirmServerCertificateReplacedCommand)}'.");

                    if (Engine.CommandsVersionManager.CurrentClusterMinimalVersion < commandVersion)
                    {
                        // If some nodes run the old version of the command, this node (newer version) will finish here and delete 'server/cert'
                        // because the last stage of the new version (ConfirmServerCertificateReplacedCommand where we delete 'server/cert') will not happen
                        using (var tx = context.OpenWriteTransaction())
                        {
                            ClusterStateMachine.DeleteItem(context, CertificateReplacement.CertificateReplacementDoc);
                            tx.Commit();
                        }

                        return;
                    }

                    await SendToLeaderAsync(new ConfirmServerCertificateReplacedCommand(newClusterCertificate.Thumbprint, oldThumbprint));
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to process {type}.", e);

                NotificationCenter.Add(AlertRaised.Create(
                    null,
                    CertificateReplacement.CertReplaceAlertTitle,
                    $"Failed to process {type}.",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        public IEnumerable<string> GetSecretKeysNames(TransactionOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree("SecretKeys");
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    yield return it.CurrentKey.ToString();
                } while (it.MoveNext());
            }
        }

        public unsafe void PutSecretKey(string base64, string name, bool overwrite)
        {
            var key = Convert.FromBase64String(base64);
            if (key.Length != 256 / 8)
                throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");

            fixed (char* pBase64 = base64)
            fixed (byte* pKey = key)
            {
                try
                {
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        PutSecretKey(ctx, name, key, overwrite);
                        tx.Commit();
                    }
                }
                finally
                {
                    Sodium.sodium_memzero((byte*)pBase64, (UIntPtr)(base64.Length * sizeof(char)));
                    Sodium.sodium_memzero(pKey, (UIntPtr)key.Length);
                }
            }
        }

        public unsafe void PutSecretKey(
            TransactionOperationContext context,
            string name,
            byte[] secretKey,
            bool overwrite = false, /* Be careful with this one, overwriting a key might be disastrous */
            bool cloneKey = false)
        {
            Debug.Assert(context.Transaction != null);

            //This will prevent the insertion of an encryption key for a database that resides in a server without encryption license.
            LicenseManager.AssertCanCreateEncryptedDatabase();

            if (secretKey.Length != 256 / 8)
                throw new ArgumentException($"Key size must be 256 bits, but was {secretKey.Length * 8}", nameof(secretKey));

            byte[] key;
            if (cloneKey)
                key = secretKey.ToArray(); // clone
            else
                key = secretKey;

            byte[] existingKey;
            try
            {
                existingKey = GetSecretKey(context, name);
            }
            catch (Exception)
            {
                // failure to read the key might be because the user password has changed
                // in this case, we ignore the existence of the key and overwrite it
                existingKey = null;
            }
            if (existingKey != null)
            {
                fixed (byte* pKey = key)
                fixed (byte* pExistingKey = existingKey)
                {
                    bool areEqual = Sodium.sodium_memcmp(pKey, pExistingKey, (UIntPtr)key.Length) == 0;
                    Sodium.sodium_memzero(pExistingKey, (UIntPtr)key.Length);
                    if (areEqual)
                    {
                        Sodium.sodium_memzero(pKey, (UIntPtr)key.Length);
                        return;
                    }
                }
            }

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");

            if (overwrite == false && tree.Read(name) != null)
                throw new InvalidOperationException($"Attempt to overwrite secret key {name}, which isn\'t permitted (you\'ll lose access to the encrypted db).");

            using (var rawRecord = Cluster.ReadRawDatabaseRecord(context, name))
            {
                if (rawRecord != null && rawRecord.IsEncrypted == false)
                    throw new InvalidOperationException($"Cannot modify key {name} where there is an existing database that is not encrypted");
            }

            fixed (byte* pKey = key)
            {
                try
                {
                    var protectedData = Secrets.Protect(key);

                    tree.Add(name, protectedData);
                }
                finally
                {
                    Sodium.sodium_memzero(pKey, (UIntPtr)key.Length);
                }
            }
        }

        public byte[] GetSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            var tree = context.Transaction.InnerTransaction.ReadTree("SecretKeys");

            var readResult = tree?.Read(name);
            if (readResult == null)
                return null;

            var protectedData = new byte[readResult.Reader.Length];
            readResult.Reader.Read(protectedData, 0, protectedData.Length);

            return Secrets.Unprotect(protectedData);
        }

        public byte[] GetSecretKey(string databaseName)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetSecretKey(context, databaseName);
            }
        }

        public void DeleteSecretKey(string databaseName)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                DeleteSecretKey(context, databaseName);
                tx.Commit();
            }
        }

        public void DeleteSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            name = ShardHelper.ToDatabaseName(name);
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(context, name))
            {
                if (CanDeleteSecretKey() == false)
                {
                    if (rawRecord.IsSharded)
                        return;

                    throw new InvalidOperationException(
                        $"Can't delete secret key for a database ({name}) that is relevant for this node ({NodeTag}), please delete the database before deleting the secret key.");
                }

                bool CanDeleteSecretKey()
                {
                    if (rawRecord == null)
                        return true;

                    if (rawRecord.IsEncrypted == false)
                        return true;

                    if (rawRecord.IsSharded)
                    {
                        List<int> shardsOnThisNode = new();
                        foreach ((int shardNumber, var shardTopology) in rawRecord.Sharding.Shards)
                        {
                            if (shardTopology.RelevantFor(NodeTag) == false)
                                continue;
                            shardsOnThisNode.Add(shardNumber);
                        }

                        if (shardsOnThisNode.Count == 0)
                            return true;

                        if (rawRecord.DeletionInProgress == null)
                            return false;

                        return shardsOnThisNode.All(shardNumber =>
                            rawRecord.DeletionInProgress.ContainsKey(DatabaseRecord.GetKeyForDeletionInProgress(NodeTag, shardNumber)));
                    }

                    if (rawRecord.Topology.RelevantFor(NodeTag) == false)
                        return true;

                    var deletionInProgress = rawRecord.DeletionInProgress;
                    if (deletionInProgress != null && deletionInProgress.ContainsKey(NodeTag))
                    {
                        // we delete the node tag from the topology only after we get a confirmation that the database was actually deleted
                        // until then, the NodeTag is in DeletionInProgress
                        return true;
                    }

                    return false;
                }
            }

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");
            tree.Delete(name);
        }

        public Task<(long Index, object Result)> DeleteDatabaseAsync(string db, bool hardDelete, string[] fromNodes, string raftRequestId)
        {
            var deleteCommand = new DeleteDatabaseCommand(db, raftRequestId)
            {
                HardDelete = hardDelete,
                FromNodes = fromNodes
            };
            return SendToLeaderAsync(deleteCommand);
        }

        public Task<(long Index, object Result)> UpdateExternalReplication(string dbName, BlittableJsonReaderObject blittableJson, string raftRequestId, out ExternalReplication watcher)
        {
            if (blittableJson.TryGet(nameof(UpdateExternalReplicationCommand.Watcher), out BlittableJsonReaderObject watcherBlittable) == false)
            {
                throw new InvalidDataException($"{nameof(UpdateExternalReplicationCommand.Watcher)} was not found.");
            }

            watcher = JsonDeserializationClient.ExternalReplication(watcherBlittable);
            Server.ServerStore.LicenseManager.AssertCanAddExternalReplication(watcher.DelayReplicationFor);

            var addWatcherCommand = new UpdateExternalReplicationCommand(dbName, raftRequestId)
            {
                Watcher = watcher
            };
            return SendToLeaderAsync(addWatcherCommand);
        }

        public Task<(long Index, object Result)> UpdatePullReplicationAsSink(string dbName, BlittableJsonReaderObject blittableJson, string raftRequestId, out PullReplicationAsSink pullReplicationAsSink)
        {
            if (blittableJson.TryGet(nameof(UpdatePullReplicationAsSinkCommand.PullReplicationAsSink), out BlittableJsonReaderObject pullReplicationBlittable) == false)
            {
                throw new InvalidDataException($"{nameof(UpdatePullReplicationAsSinkCommand.PullReplicationAsSink)} was not found.");
            }

            pullReplicationAsSink = JsonDeserializationClient.PullReplicationAsSink(pullReplicationBlittable);

            var replicationAsSinkCommand = new UpdatePullReplicationAsSinkCommand(dbName, raftRequestId)
            {
                PullReplicationAsSink = pullReplicationAsSink,
                // JsonDeserializationClient assign null value for null value and for missing property.
                // we need to know if we want to use server certificate or not to change the current one.
                // null value for server certificate
                // missing property for 'do not change'
                UseServerCertificate = (pullReplicationAsSink.CertificateWithPrivateKey == null) &&
                    (pullReplicationBlittable.TryGet(nameof(PullReplicationAsSink.CertificateWithPrivateKey), out string _))
            };


            return SendToLeaderAsync(replicationAsSinkCommand);
        }

        public Task<(long Index, object Result)> DeleteOngoingTask(long taskId, string taskName, OngoingTaskType taskType, string dbName, string raftRequestId)
        {
            var deleteTaskCommand =
                taskType == OngoingTaskType.Subscription ?
                    (CommandBase)new DeleteSubscriptionCommand(dbName, taskName, raftRequestId) :
                    new DeleteOngoingTaskCommand(taskId, taskType, dbName, raftRequestId);

            return SendToLeaderAsync(deleteTaskCommand);
        }

        public Task<(long Index, object Result)> PromoteDatabaseNode(string dbName, string nodeTag, string raftRequestId)
        {
            var promoteDatabaseNodeCommand = new PromoteDatabaseNodeCommand(dbName, raftRequestId)
            {
                NodeTag = nodeTag
            };
            return SendToLeaderAsync(promoteDatabaseNodeCommand);
        }

        public Task<(long Index, object Result)> ModifyConflictSolverAsync(string dbName, ConflictSolver solver, string raftRequestId)
        {
            var conflictResolverCommand = new ModifyConflictSolverCommand(dbName, raftRequestId)
            {
                Solver = solver
            };
            return SendToLeaderAsync(conflictResolverCommand);
        }

        public Task<(long Index, object Result)> PutValueInClusterAsync<T>(PutValueCommand<T> cmd)
        {
            return SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> ModifyDatabaseExpiration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var expiration = JsonDeserializationCluster.ExpirationConfiguration(configurationJson);
            if (expiration.DeleteFrequencyInSec <= 0)
            {
                throw new InvalidOperationException(
                    $"Expiration delete frequency for database '{databaseName}' must be greater than 0.");
            }
            var editExpiration = new EditExpirationCommand(expiration, databaseName, raftRequestId);
            return SendToLeaderAsync(editExpiration);
        }

        public Task<(long Index, object Result)> ModifyDatabaseDataArchival(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var dataArchivalConfiguration = JsonDeserializationCluster.DataArchivalConfiguration(configurationJson);
            if (dataArchivalConfiguration.ArchiveFrequencyInSec <= 0)
            {
                throw new InvalidOperationException(
                    $"Archive frequency for database '{databaseName}' must be greater than 0.");
            }
            var editDataArchival = new EditDataArchivalCommand(dataArchivalConfiguration, databaseName, raftRequestId);
            return SendToLeaderAsync(editDataArchival);
        }

        public Task<(long Index, object Result)> ModifyDocumentsCompression(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var documentsCompression = JsonDeserializationCluster.DocumentsCompressionConfiguration(configurationJson);

            LicenseManager.AssertCanUseDocumentsCompression(documentsCompression);

            var editDocumentsCompression = new EditDocumentsCompressionCommand(documentsCompression, databaseName, raftRequestId);
            return SendToLeaderAsync(editDocumentsCompression);
        }

        public Task<(long Index, object Result)> ModifyPostgreSqlConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var config = JsonDeserializationCluster.PostgreSqlConfiguration(configurationJson);

            return ModifyPostgreSqlConfiguration(context, databaseName, config, raftRequestId);
        }

        public Task<(long Index, object Result)> ModifyPostgreSqlConfiguration(TransactionOperationContext context, string databaseName, PostgreSqlConfiguration configuration, string raftRequestId)
        {
            var editPostgreSqlConfiguration = new EditPostgreSqlConfigurationCommand(configuration, databaseName, raftRequestId);
            return SendToLeaderAsync(editPostgreSqlConfiguration);
        }

        public Task<(long Index, object Result)> ModifyDatabaseRefresh(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var refresh = JsonDeserializationCluster.RefreshConfiguration(configurationJson);
            if (refresh.RefreshFrequencyInSec <= 0)
            {
                throw new InvalidOperationException(
                    $"Refresh frequency for database '{databaseName}' must be greater than 0.");
            }
            var editExpiration = new EditRefreshCommand(refresh, databaseName, raftRequestId);
            return SendToLeaderAsync(editExpiration);
        }

        public Task<(long Index, object Result)> ToggleDatabasesStateAsync(ToggleDatabasesStateCommand.Parameters.ToggleType toggleType, string[] databaseNames, bool disable, string raftRequestId)
        {
            var command = new ToggleDatabasesStateCommand(new ToggleDatabasesStateCommand.Parameters
            {
                Type = toggleType,
                DatabaseNames = databaseNames,
                Disable = disable
            }, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public Task<(long Index, object Result)> PutServerWideBackupConfigurationAsync(ServerWideBackupConfiguration configuration, string raftRequestId)
        {
            var command = new PutServerWideBackupConfigurationCommand(configuration, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public Task<(long Index, object Result)> PutServerWideExternalReplicationAsync(ServerWideExternalReplication configuration, string raftRequestId)
        {
            var command = new PutServerWideExternalReplicationCommand(configuration, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public Task<(long Index, object Result)> DeleteServerWideTaskAsync(DeleteServerWideTaskCommand.DeleteConfiguration configuration, string raftRequestId)
        {
            var command = new DeleteServerWideTaskCommand(configuration, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public Task<(long Index, object Result)> ToggleServerWideTaskStateAsync(ToggleServerWideTaskStateCommand.Parameters configuration, string raftRequestId)
        {
            var command = new ToggleServerWideTaskStateCommand(configuration, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public async Task<(long, object)> ModifyPeriodicBackup(TransactionOperationContext context, string name, PeriodicBackupConfiguration configuration, string raftRequestId)
        {
            var modifyPeriodicBackup = new UpdatePeriodicBackupCommand(configuration, name, raftRequestId);
            return await SendToLeaderAsync(modifyPeriodicBackup);
        }

        public async Task<(long, object)> AddEtl(TransactionOperationContext context,
            string databaseName, BlittableJsonReaderObject etlConfiguration, string raftRequestId)
        {
            UpdateDatabaseCommand command;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, databaseName))
            {
                switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
                {
                    case EtlType.Raven:
                        var rvnEtl = JsonDeserializationCluster.RavenEtlConfiguration(etlConfiguration);
                        rvnEtl.Validate(out var rvnEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, rvnEtl.ConnectionStringName, rvnEtl.EtlType) == false)
                            rvnEtlErr.Add($"Could not find connection string named '{rvnEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, rvnEtlErr);

                        command = new AddRavenEtlCommand(rvnEtl, databaseName, raftRequestId);
                        break;

                    case EtlType.Sql:
                        var sqlEtl = JsonDeserializationCluster.SqlEtlConfiguration(etlConfiguration);
                        sqlEtl.Validate(out var sqlEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, sqlEtl.ConnectionStringName, sqlEtl.EtlType) == false)
                            sqlEtlErr.Add($"Could not find connection string named '{sqlEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, sqlEtlErr);

                        command = new AddSqlEtlCommand(sqlEtl, databaseName, raftRequestId);
                        break;

                    case EtlType.Olap:
                        var olapEtl = JsonDeserializationCluster.OlapEtlConfiguration(etlConfiguration);
                        olapEtl.Validate(out var olapEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, olapEtl.ConnectionStringName, olapEtl.EtlType) == false)
                            olapEtlErr.Add($"Could not find connection string named '{olapEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, olapEtlErr);

                        command = new AddOlapEtlCommand(olapEtl, databaseName, raftRequestId);
                        break;

                    case EtlType.ElasticSearch:
                        var elasticSearchEtl = JsonDeserializationCluster.ElasticSearchEtlConfiguration(etlConfiguration);
                        elasticSearchEtl.Validate(out var elasticEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, elasticSearchEtl.ConnectionStringName, elasticSearchEtl.EtlType) == false)
                            elasticEtlErr.Add($"Could not find connection string named '{elasticSearchEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, elasticEtlErr);

                        command = new AddElasticSearchEtlCommand(elasticSearchEtl, databaseName, raftRequestId);
                        break;

                    case EtlType.Queue:
                        var queueEtl = JsonDeserializationCluster.QueueEtlConfiguration(etlConfiguration);
                        queueEtl.Validate(out var queueEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, queueEtl.ConnectionStringName, queueEtl.EtlType) == false)
                            queueEtlErr.Add($"Could not find connection string named '{queueEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, queueEtlErr);

                        command = new AddQueueEtlCommand(queueEtl, databaseName, raftRequestId);
                        break;

                    default:
                        throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
                }
            }

            return await SendToLeaderAsync(command);
        }

        public async Task<(long, object)> AddQueueSink(TransactionOperationContext context,
            string databaseName, BlittableJsonReaderObject queueSinkConfiguration, string raftRequestId)
        {
            UpdateDatabaseCommand command;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, databaseName))
            {
                var queueSink = JsonDeserializationCluster.QueueSinkConfiguration(queueSinkConfiguration);
                queueSink.Validate(out var queueSinkErr, validateName: false, validateConnection: false);

                var queueConnectionString = rawRecord.QueueConnectionStrings;
                var validateConnectionString = queueConnectionString != null && queueConnectionString.TryGetValue(queueSink.ConnectionStringName, out _);

                if (validateConnectionString == false)
                    queueSinkErr.Add($"Could not find connection string named '{queueSink.ConnectionStringName}'. Please supply an existing connection string.");

                ThrowInvalidQueueSinkConfigurationIfNecessary(queueSinkConfiguration, queueSinkErr);
                command = new AddQueueSinkCommand(queueSink, databaseName, raftRequestId);
            }

            return await SendToLeaderAsync(command);
        }

        public async Task<(long, object)> UpdateQueueSink(TransactionOperationContext context, string databaseName,
            long id, BlittableJsonReaderObject queueSinkConfiguration, string raftRequestId)
        {
            UpdateDatabaseCommand command;
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, databaseName))
            {
                var queueSink = JsonDeserializationCluster.QueueSinkConfiguration(queueSinkConfiguration);
                queueSink.Validate(out var queueSinkErr, validateName: false, validateConnection: false);

                var queueConnectionString = rawRecord.QueueConnectionStrings;
                var result = queueConnectionString != null && queueConnectionString.TryGetValue(queueSink.ConnectionStringName, out _);

                if (result == false)
                    queueSinkErr.Add($"Could not find connection string named '{queueSink.ConnectionStringName}'. Please supply an existing connection string.");

                ThrowInvalidQueueSinkConfigurationIfNecessary(queueSinkConfiguration, queueSinkErr);
                command = new UpdateQueueSinkCommand(id, queueSink, databaseName, raftRequestId);
            }

            return await SendToLeaderAsync(command);
        }

        [DoesNotReturn]
        private void ThrowInvalidConfigurationIfNecessary(BlittableJsonReaderObject etlConfiguration, IReadOnlyCollection<string> errors)
        {
            if (errors.Count <= 0)
                return;

            var sb = new StringBuilder();
            sb
                .AppendLine("Invalid ETL configuration.")
                .AppendLine("Errors:");

            foreach (var err in errors)
            {
                sb
                    .Append("- ")
                    .AppendLine(err);
            }

            sb.AppendLine("Configuration:");
            sb.AppendLine(etlConfiguration.ToString());

            throw new InvalidOperationException(sb.ToString());
        }

        private void ThrowInvalidQueueSinkConfigurationIfNecessary(BlittableJsonReaderObject queueSinkConfiguration,
            IReadOnlyCollection<string> errors)
        {
            if (errors.Count <= 0)
                return;

            var sb = new StringBuilder();
            sb
                .AppendLine("Invalid Queue Sink configuration.")
                .AppendLine("Errors:");

            foreach (var err in errors)
            {
                sb
                    .Append("- ")
                    .AppendLine(err);
            }

            sb.AppendLine("Configuration:");
            sb.AppendLine(queueSinkConfiguration.ToString());

            throw new InvalidOperationException(sb.ToString());
        }

        private bool ValidateConnectionString(RawDatabaseRecord databaseRecord, string connectionStringName, EtlType etlType)
        {
            switch (etlType)
            {
                case EtlType.Raven:
                    var ravenConnectionStrings = databaseRecord.RavenConnectionStrings;
                    return ravenConnectionStrings != null && ravenConnectionStrings.TryGetValue(connectionStringName, out _);
                case EtlType.Sql:
                    var sqlConnectionString = databaseRecord.SqlConnectionStrings;
                    return sqlConnectionString != null && sqlConnectionString.TryGetValue(connectionStringName, out _);
                case EtlType.Olap:
                    var olapConnectionString = databaseRecord.OlapConnectionString;
                    return olapConnectionString != null && olapConnectionString.TryGetValue(connectionStringName, out _);
                case EtlType.ElasticSearch:
                    var elasticSearchConnectionString = databaseRecord.ElasticSearchConnectionStrings;
                    return elasticSearchConnectionString != null && elasticSearchConnectionString.TryGetValue(connectionStringName, out _);
                case EtlType.Queue:
                    var queueConnectionString = databaseRecord.QueueConnectionStrings;
                    return queueConnectionString != null && queueConnectionString.TryGetValue(connectionStringName, out _);
                default:
                    throw new NotSupportedException($"Unknown ETL type. Type: {etlType}");
            }
        }

        public async Task<(long, object)> UpdateEtl(TransactionOperationContext context, string databaseName, long id, BlittableJsonReaderObject etlConfiguration, string raftRequestId)
        {
            UpdateDatabaseCommand command;
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, databaseName))
            {
                switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
                {
                    case EtlType.Raven:
                        var rvnEtl = JsonDeserializationCluster.RavenEtlConfiguration(etlConfiguration);
                        rvnEtl.Validate(out var rvnEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, rvnEtl.ConnectionStringName, rvnEtl.EtlType) == false)
                            rvnEtlErr.Add($"Could not find connection string named '{rvnEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, rvnEtlErr);

                        command = new UpdateRavenEtlCommand(id, rvnEtl, databaseName, raftRequestId);
                        break;
                    case EtlType.Sql:
                        var sqlEtl = JsonDeserializationCluster.SqlEtlConfiguration(etlConfiguration);
                        sqlEtl.Validate(out var sqlEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, sqlEtl.ConnectionStringName, sqlEtl.EtlType) == false)
                            sqlEtlErr.Add($"Could not find connection string named '{sqlEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, sqlEtlErr);

                        command = new UpdateSqlEtlCommand(id, sqlEtl, databaseName, raftRequestId);
                        break;
                    case EtlType.Olap:
                        var olapEtl = JsonDeserializationCluster.OlapEtlConfiguration(etlConfiguration);
                        olapEtl.Validate(out var olapEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, olapEtl.ConnectionStringName, olapEtl.EtlType) == false)
                            olapEtlErr.Add($"Could not find connection string named '{olapEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, olapEtlErr);

                        command = new UpdateOlapEtlCommand(id, olapEtl, databaseName, raftRequestId);
                        break;
                    case EtlType.ElasticSearch:
                        var elasticSearchEtl = JsonDeserializationCluster.ElasticSearchEtlConfiguration(etlConfiguration);
                        elasticSearchEtl.Validate(out var elasticSearchEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, elasticSearchEtl.ConnectionStringName, elasticSearchEtl.EtlType) == false)
                            elasticSearchEtlErr.Add($"Could not find connection string named '{elasticSearchEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, elasticSearchEtlErr);

                        command = new UpdateElasticSearchEtlCommand(id, elasticSearchEtl, databaseName, raftRequestId);
                        break;
                    case EtlType.Queue:
                        var queueEtl = JsonDeserializationCluster.QueueEtlConfiguration(etlConfiguration);
                        queueEtl.Validate(out var queueEtlErr, validateName: false, validateConnection: false);
                        if (ValidateConnectionString(rawRecord, queueEtl.ConnectionStringName, queueEtl.EtlType) == false)
                            queueEtlErr.Add($"Could not find connection string named '{queueEtl.ConnectionStringName}'. Please supply an existing connection string.");

                        ThrowInvalidConfigurationIfNecessary(etlConfiguration, queueEtlErr);

                        command = new UpdateQueueEtlCommand(id, queueEtl, databaseName, raftRequestId);
                        break;
                    default:
                        throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
                }
            }

            return await SendToLeaderAsync(command);
        }

        public Task<(long, object)> RemoveEtlProcessState(TransactionOperationContext context, string databaseName, string configurationName, string transformationName, string raftRequestId)
        {
            var command = new RemoveEtlProcessStateCommand(databaseName, configurationName, transformationName, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public Task<(long, object)> RemoveQueueSinkProcessState(TransactionOperationContext context, string databaseName, string configurationName, string scriptName, string raftRequestId)
        {
            var command = new RemoveQueueSinkProcessStateCommand(databaseName, configurationName, scriptName, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public Task<(long, object)> ModifyDatabaseRevisions(JsonOperationContext context, string name, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var editRevisions = new EditRevisionsConfigurationCommand(JsonDeserializationCluster.RevisionsConfiguration(configurationJson), name, raftRequestId);
            return SendToLeaderAsync(editRevisions);
        }

        public Task<(long, object)> ModifyRevisionsForConflicts(JsonOperationContext context, string name, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var editRevisions = new EditRevisionsForConflictsConfigurationCommand(JsonDeserializationCluster.RevisionsCollectionConfiguration(configurationJson), name, raftRequestId);
            return SendToLeaderAsync(editRevisions);
        }

        public async Task<(long, object)> PutConnectionString(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject connectionString, string raftRequestId)
        {
            UpdateDatabaseCommand command;

            var connectionStringType = ConnectionString.GetConnectionStringType(connectionString);

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    command = new PutRavenConnectionStringCommand(JsonDeserializationCluster.RavenConnectionString(connectionString), databaseName, raftRequestId);
                    break;

                case ConnectionStringType.Sql:
                    // RavenDB-21784 - Replace obsolete MySql provider name
                    var deserializedSqlConnectionString = JsonDeserializationCluster.SqlConnectionString(connectionString);
                    if (deserializedSqlConnectionString.FactoryName == "MySql.Data.MySqlClient")
                    {
                        deserializedSqlConnectionString.FactoryName = "MySqlConnector.MySqlConnectorFactory";
                        var alert = AlertRaised.Create(databaseName, "Deprecated MySql factory auto-updated", "MySql.Data.MySqlClient factory has been defaulted to MySqlConnector.MySqlConnectorFactory",
                            AlertType.SqlConnectionString_DeprecatedFactoryReplaced, NotificationSeverity.Info);
                        NotificationCenter.Add(alert);
                    }

                    command = new PutSqlConnectionStringCommand(deserializedSqlConnectionString, databaseName, raftRequestId);
                    break;
                case ConnectionStringType.Olap:
                    command = new PutOlapConnectionStringCommand(JsonDeserializationCluster.OlapConnectionString(connectionString), databaseName, raftRequestId);
                    break;
                case ConnectionStringType.ElasticSearch:
                    command = new PutElasticSearchConnectionStringCommand(JsonDeserializationCluster.ElasticSearchConnectionString(connectionString), databaseName, raftRequestId);
                    break;
                case ConnectionStringType.Queue:
                    command = new PutQueueConnectionStringCommand(JsonDeserializationCluster.QueueConnectionString(connectionString), databaseName, raftRequestId);
                    break;

                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
            }

            return await SendToLeaderAsync(command);
        }

        public async Task<(long, object)> RemoveConnectionString(string databaseName, string connectionStringName, string type, string raftRequestId)
        {
            if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

            UpdateDatabaseCommand command;
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, databaseName))
            {
                switch (connectionStringType)
                {
                    case ConnectionStringType.Raven:

                        // Don't delete the connection string if used by tasks types: External Replication || Raven Etl

                        var ravenEtls = rawRecord.RavenEtls;
                        if (ravenEtls != null)
                        {
                            foreach (var ravenETlTask in ravenEtls)
                            {
                                if (ravenETlTask.ConnectionStringName == connectionStringName)
                                {
                                    throw new InvalidOperationException(
                                        $"Can't delete connection string: {connectionStringName}. It is used by task: {ravenETlTask.Name}");
                                }
                            }
                        }

                        var externalReplications = rawRecord.ExternalReplications;
                        if (externalReplications != null)
                        {
                            foreach (var replicationTask in externalReplications)
                            {
                                if (replicationTask.ConnectionStringName == connectionStringName)
                                {
                                    throw new InvalidOperationException(
                                        $"Can't delete connection string: {connectionStringName}. It is used by task: {replicationTask.Name}");
                                }
                            }
                        }

                        command = new RemoveRavenConnectionStringCommand(connectionStringName, databaseName, raftRequestId);
                        break;

                    case ConnectionStringType.Sql:

                        var sqlEtls = rawRecord.SqlEtls;

                        // Don't delete the connection string if used by tasks types: SQL Etl
                        if (sqlEtls != null)
                        {
                            foreach (var sqlETlTask in sqlEtls)
                            {
                                if (sqlETlTask.ConnectionStringName == connectionStringName)
                                {
                                    throw new InvalidOperationException($"Can't delete connection string: {connectionStringName}. It is used by task: {sqlETlTask.Name}");
                                }
                            }
                        }

                        command = new RemoveSqlConnectionStringCommand(connectionStringName, databaseName, raftRequestId);
                        break;

                    case ConnectionStringType.Olap:

                        var olapEtls = rawRecord.OlapEtls;

                        // Don't delete the connection string if used by tasks types: Olap Etl
                        if (olapEtls != null)
                        {
                            foreach (var olapETlTask in olapEtls)
                            {
                                if (olapETlTask.ConnectionStringName == connectionStringName)
                                {
                                    throw new InvalidOperationException($"Can't delete connection string: {connectionStringName}. It is used by task: {olapETlTask.Name}");
                                }
                            }
                        }

                        command = new RemoveOlapConnectionStringCommand(connectionStringName, databaseName, raftRequestId);
                        break;

                    case ConnectionStringType.ElasticSearch:

                        var elasticSearchEtls = rawRecord.ElasticSearchEtls;

                        // Don't delete the connection string if used by tasks types: ElasticSearch Etl
                        if (elasticSearchEtls != null)
                        {
                            foreach (var elasticSearchETlTask in elasticSearchEtls)
                            {
                                if (elasticSearchETlTask.ConnectionStringName == connectionStringName)
                                {
                                    throw new InvalidOperationException(
                                        $"Can't delete connection string: {connectionStringName}. It is used by task: {elasticSearchETlTask.Name}");
                                }
                            }
                        }

                        command = new RemoveElasticSearchConnectionStringCommand(connectionStringName, databaseName, raftRequestId);
                        break;

                    case ConnectionStringType.Queue:

                        var queueEtls = rawRecord.QueueEtls;

                        // Don't delete the connection string if used by tasks types: Queue Etl
                        if (queueEtls != null)
                        {
                            foreach (var queueEtlTask in queueEtls)
                            {
                                if (queueEtlTask.ConnectionStringName == connectionStringName)
                                {
                                    throw new InvalidOperationException(
                                        $"Can't delete connection string: {connectionStringName}. It is used by task: {queueEtlTask.Name}");
                                }
                            }
                        }

                        command = new RemoveQueueConnectionStringCommand(connectionStringName, databaseName, raftRequestId);
                        break;

                    default:
                        throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
                }
            }

            return await SendToLeaderAsync(command);
        }

        public Guid GetServerId()
        {
            return _env.DbId;
        }

        public Guid ServerId => GetServerId();

        public char DefaultIdentityPartsSeparator;

        public bool IsShutdownRequested()
        {
            return _shutdownNotification.IsCancellationRequested;
        }

        public void Dispose()
        {
            if (_shutdownNotification.IsCancellationRequested || _disposed)
                return;

            lock (this)
            {
                if (_disposed)
                    return;

                try
                {
                    if (_shutdownNotification.IsCancellationRequested)
                        return;

                    _shutdownNotification.Cancel();

                    if (ContextPool != null)
                    {
                        _server.Statistics.Persist(ContextPool, Logger);
                    }

                    _server.ServerCertificateChanged -= OnServerCertificateChanged;

                    var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {nameof(ServerStore)}.");

                    exceptionAggregator.Execute(() =>
                    {
                        try
                        {
                            _engine?.Dispose();
                        }
                        catch (ObjectDisposedException)
                        {
                            //we are disposing, so don't care
                        }
                    });

                    exceptionAggregator.Execute(() =>
                    {
                        if (_clusterMaintenanceSetupTask != null && _clusterMaintenanceSetupTask != PoolOfThreads.LongRunningWork.Current)
                            _clusterMaintenanceSetupTask.Join(int.MaxValue);
                    });

                    exceptionAggregator.Execute(() =>
                    {
                        if (_updateTopologyChangeNotification != null && _updateTopologyChangeNotification != PoolOfThreads.LongRunningWork.Current)
                            _updateTopologyChangeNotification.Join(int.MaxValue);
                    });

                    var toDispose = new List<IDisposable>
                    {
                        StorageSpaceMonitor,
                        ServerLimitsMonitor,
                        NotificationCenter,
                        LicenseManager,
                        DatabasesLandlord,
                        _env,
                        _leaderRequestExecutor,
                        ContextPool,
                        ByteStringMemoryCache.Cleaner,
                        InitializationCompleted,
                        _fileLocker
                    };

                    foreach (var disposable in toDispose)
                        exceptionAggregator.Execute(() =>
                        {
                            try
                            {
                                disposable?.Dispose();
                            }
                            catch (ObjectDisposedException)
                            {
                                //we are disposing, so don't care
                            }
                            catch (DatabaseDisabledException)
                            {
                            }
                        });

                    exceptionAggregator.Execute(_shutdownNotification.Dispose);

                    exceptionAggregator.Execute(() => _timer?.Dispose());

                    exceptionAggregator.Execute(() =>
                    {
                        if (_clusterRequestExecutor?.IsValueCreated == true)
                            _clusterRequestExecutor.Value.Dispose();
                    });

                    exceptionAggregator.ThrowIfNeeded();
                }
                finally
                {
                    _disposed = true;
                }
            }
        }

        public void IdleOperations(object state)
        {
            try
            {
                foreach (var db in DatabasesLandlord.DatabasesCache)
                {
                    try
                    {
                        if (db.Value.Status != TaskStatus.RanToCompletion)
                            continue;

                        var database = db.Value.Result;

                        if (DatabaseNeedsToRunIdleOperations(database, out var mode))
                            database.RunIdleOperations(mode);
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error during idle operation run for " + db.Key, e);
                    }
                }

                try
                {
                    _server.Statistics.MaybePersist(ContextPool, Logger);

                    foreach (var databaseKvp in DatabasesLandlord.LastRecentlyUsed.ForceEnumerateInThreadSafeManner())
                    {
                        if (CanUnloadDatabase(databaseKvp.Key, databaseKvp.Value, statistics: null, out DocumentDatabase database) == false)
                            continue;

                        var dbIdEtagDictionary = new Dictionary<string, long>();
                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                        using (documentsContext.OpenReadTransaction())
                        {
                            foreach (var kvp in DocumentsStorage.GetAllReplicatedEtags(documentsContext))
                                dbIdEtagDictionary[kvp.Key] = kvp.Value;
                        }

                        if (DatabasesLandlord.UnloadDirectly(databaseKvp.Key, database.PeriodicBackupRunner.GetNextIdleDatabaseActivity(database.Name)))
                            IdleDatabases[database.Name] = dbIdEtagDictionary;
                    }
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Error during idle operations for the server", e);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Unexpected error during idle operations for the server", e);
            }
            finally
            {
                try
                {
                    _timer.Change(_frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        public bool CanUnloadDatabase(StringSegment databaseName, DateTime lastRecentlyUsed, DatabasesDebugHandler.IdleDatabaseStatistics statistics, out DocumentDatabase database)
        {
            database = null;
            var now = SystemTime.UtcNow;

            if (statistics != null)
                statistics.LastRecentlyUsed = lastRecentlyUsed;

            var diff = now - lastRecentlyUsed;

            if (DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> resourceTask) == false
                || resourceTask == null
                || resourceTask.Status != TaskStatus.RanToCompletion)
            {
                if (statistics != null)
                {
                    statistics.IsLoaded = false;
                    statistics.Explanations.Add("Cannot unload database because it is not loaded yet.");
                }

                return false;
            }

            database = resourceTask.Result;

            var maxTimeDatabaseCanBeIdle = database.Configuration.Databases.MaxIdleTime.AsTimeSpan;

            if (statistics != null)
                statistics.MaxIdleTime = maxTimeDatabaseCanBeIdle;

            if (diff <= maxTimeDatabaseCanBeIdle)
            {
                if (statistics == null)
                    return false;
                else
                {
                    statistics.Explanations.Add($"Cannot unload database because the difference ({diff}) between now ({now}) and last recently used ({lastRecentlyUsed}) is lower or equal to max idle time ({maxTimeDatabaseCanBeIdle}).");
                }
            }

            if (statistics != null)
                statistics.IsLoaded = true;

            // intentionally inside the loop, so we get better concurrency overall
            // since shutting down a database can take a while
            if (database.Configuration.Core.RunInMemory)
            {
                if (statistics != null)
                {
                    statistics.RunInMemory = true;
                    statistics.Explanations.Add("Cannot unload database because it is running in memory.");
                }

                return false;
            }

            var canUnload = database.CanUnload;

            if (statistics != null)
                statistics.CanUnload = canUnload;

            if (canUnload == false)
            {
                if (statistics == null)
                    return false;
                else
                {
                    statistics.Explanations.Add("Cannot unload database because it explicitly cannot be unloaded.");
                }
            }

            var lastWork = DatabasesLandlord.LastWork(database);
            if (statistics != null)
                statistics.LastWork = lastWork;

            diff = now - lastWork;

            if (diff <= maxTimeDatabaseCanBeIdle)
            {
                if (statistics == null)
                    return false;
                else
                {
                    statistics.Explanations.Add($"Cannot unload database because the difference ({diff}) between now ({now}) and last work time ({lastWork}) is lower or equal to max idle time ({maxTimeDatabaseCanBeIdle}).");
                }
            }

            var numberOfChangesApiConnections = database.Changes.Connections.Values.Count(x => x.IsDisposed == false && x.IsChangesConnectionOriginatedFromStudio == false);
            if (statistics != null)
                statistics.NumberOfChangesApiConnections = numberOfChangesApiConnections;

            if (numberOfChangesApiConnections > 0)
            {
                if (statistics == null)
                    return false;
                else
                {
                    statistics.Explanations.Add($"Cannot unload database because number of Changes API connections ({numberOfChangesApiConnections}) is greater than 0");
                }
            }

            var numberOfSubscriptionConnections = database.SubscriptionStorage.GetNumberOfRunningSubscriptions();
            if (statistics != null)
                statistics.NumberOfSubscriptionConnections = numberOfSubscriptionConnections;

            if (numberOfSubscriptionConnections > 0)
            {
                if (statistics == null)
                    return false;

                statistics.Explanations.Add($"Cannot unload database because number of Subscriptions connections ({numberOfSubscriptionConnections}) is greater than 0");
            }

            var hasActiveOperations = database.Operations.HasActive;
            if (statistics != null)
                statistics.HasActiveOperations = hasActiveOperations;

            if (hasActiveOperations)
            {
                if (statistics == null)
                    return false;
                else
                {
                    statistics.Explanations.Add("Cannot unload database because it has active operations");
                }
            }

            if (statistics != null)
                return statistics.Explanations.Count == 0;

            return true;
        }

        private bool DatabaseNeedsToRunIdleOperations(DocumentDatabase database, out DatabaseCleanupMode mode)
        {
            var now = DateTime.UtcNow;

            var envs = database.GetAllStoragesEnvironment();

            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            if ((now - maxLastWork).CompareTo(database.Configuration.Databases.DeepCleanupThreshold.AsTimeSpan) > 0)
            {
                mode = DatabaseCleanupMode.Deep;
                return true;
            }

            if ((now - database.LastIdleTime).CompareTo(database.Configuration.Databases.RegularCleanupThreshold.AsTimeSpan) > 0)
            {
                mode = DatabaseCleanupMode.Regular;
                return true;
            }

            mode = DatabaseCleanupMode.None;
            return false;
        }

        public void AssignNodesToDatabase(ClusterTopology clusterTopology, string name, bool encrypted, DatabaseTopology databaseTopology)
        {
            Debug.Assert(databaseTopology != null);

            if (clusterTopology.AllNodes.Count == 0)
                throw new InvalidOperationException($"Database {name} cannot be created, because the cluster topology is empty (shouldn't happen)!");

            if (databaseTopology.ReplicationFactor == 0)
                throw new InvalidOperationException($"Database {name} cannot be created with replication factor of 0.");

            var clusterNodes = clusterTopology.Members.Keys
                .Concat(clusterTopology.Watchers.Keys)
                .ToList();

            if (encrypted && Server.AllowEncryptedDatabasesOverHttp == false)
            {
                clusterNodes.RemoveAll(n => AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(n)));
                if (clusterNodes.Count < databaseTopology.ReplicationFactor)
                    throw new InvalidOperationException(
                        $"Database {name} is encrypted and requires {databaseTopology.ReplicationFactor} node(s) which supports SSL. There are {clusterNodes.Count} such node(s) available in the cluster.");
            }

            if (clusterNodes.Count < databaseTopology.ReplicationFactor)
            {
                throw new InvalidOperationException(
                    $"Database {name} requires {databaseTopology.ReplicationFactor} node(s) but there are {clusterNodes.Count} nodes available in the cluster.");
            }

            var disconnectedNodes = new List<string>();
            foreach (var kvp in GetNodesStatuses())
            {
                var tag = kvp.Key;
                var connected = kvp.Value.Connected;
                if (connected)
                    continue;

                if (clusterNodes.Remove(tag))
                {
                    disconnectedNodes.Add(tag);
                }
            }

            var offset = new Random().Next();

            // first we would prefer the connected nodes
            var factor = databaseTopology.ReplicationFactor;
            var count = Math.Min(clusterNodes.Count, factor);
            for (var i = 0; i < count; i++)
            {
                factor--;
                var selectedNode = clusterNodes[(i + offset) % clusterNodes.Count];
                databaseTopology.Members.Add(selectedNode);
            }

            // only if all the online nodes are occupied, try to place on the disconnected
            for (int i = 0; i < Math.Min(disconnectedNodes.Count, factor); i++)
            {
                var selectedNode = disconnectedNodes[(i + offset) % disconnectedNodes.Count];
                databaseTopology.Members.Add(selectedNode);
            }
        }

        public async Task<(long Index, object Result)> WriteDatabaseRecordAsync(
            string databaseName, DatabaseRecord record, long? index, string raftRequestId,
            Dictionary<string, BlittableJsonReaderObject> databaseValues = null, bool isRestore = false, int replicationFactor = 1)
        {
            databaseValues ??= new Dictionary<string, BlittableJsonReaderObject>();

            using (Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (isRestore == false)
                {
                    var dbRecordExist = Cluster.DatabaseExists(context, databaseName);
                    if (index.HasValue && dbRecordExist == false)
                        throw new BadRequestException($"Attempted to modify non-existing database: '{databaseName}'");

                    if (dbRecordExist && index.HasValue == false)
                        throw new ConcurrencyException($"Database '{databaseName}' already exists!");
                }

                DatabaseHelper.FillDatabaseTopology(this, context, databaseName, record, replicationFactor, index, isRestore);
            }

            var addDatabaseCommand = new AddDatabaseCommand(raftRequestId)
            {
                Name = databaseName,
                RaftCommandIndex = index,
                Record = record,
                DatabaseValues = databaseValues,
                IsRestore = isRestore
            };

            return await SendToLeaderAsync(addDatabaseCommand);
        }

        public async Task EnsureNotPassiveAsync(string publicServerUrl = null, string nodeTag = "A", bool skipLicenseActivation = false)
        {
            if (_engine.CurrentState != RachisState.Passive)
                return;

            if (_engine.Bootstrap(publicServerUrl ?? _server.ServerStore.GetNodeHttpServerUrl(), nodeTag) == false)
                return;

            if (skipLicenseActivation == false)
                await LicenseManager.TryActivateLicenseAsync(Server.ThrowOnLicenseActivationFailure);

            // we put a certificate in the local state to tell the server who to trust, and this is done before
            // the cluster exists (otherwise the server won't be able to receive initial requests). Only when we
            // create the cluster, we register those local certificates in the cluster.
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                long? index = null;
                using (ctx.OpenReadTransaction())
                {
                    foreach (var localCertKey in Cluster.GetCertificateThumbprintsFromLocalState(ctx))
                    {
                        // if there are trusted certificates in the local state, we will register them in the cluster now
                        using (var localCertificate = Cluster.GetLocalStateByThumbprint(ctx, localCertKey))
                        {
                            var certificateDefinition = JsonDeserializationServer.CertificateDefinition(localCertificate);
                            var (newIndex, _) = await PutValueInClusterAsync(new PutCertificateCommand(localCertKey, certificateDefinition, RaftIdGenerator.NewId()));
                            index = newIndex;
                        }
                    }
                }

                if (index.HasValue)
                    await Cluster.WaitForIndexNotification(index.Value);
            }

            Debug.Assert(_engine.CurrentState != RachisState.Passive, "_engine.CurrentState != RachisState.Passive");
        }

        public bool IsLeader()
        {
            return _engine.CurrentState == RachisState.Leader;
        }

        public bool IsPassive()
        {
            return _engine.CurrentState == RachisState.Passive;
        }

        public DynamicJsonArray GetClusterErrors()
        {
            return _engine.GetClusterErrorsFromLeader();
        }

        public async Task<(long ClusterEtag, string ClusterId, long newIdentityValue)> GenerateClusterIdentityAsync(string id, char identityPartsSeparator, string databaseName, string raftRequestId)
        {
            var (etag, result) = await SendToLeaderAsync(new IncrementClusterIdentityCommand(databaseName, id.ToLower(), raftRequestId));

            if (result == null)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should generate a cluster-wide identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            return (etag, id.Substring(0, id.Length - 1) + identityPartsSeparator + result, (long)result);
        }

        public async Task<long> UpdateClusterIdentityAsync(string id, string databaseName, long newIdentity, bool force, string raftRequestId)
        {
            var identities = new Dictionary<string, long>
            {
                [id] = newIdentity
            };

            var (_, result) = await SendToLeaderAsync(new UpdateClusterIdentityCommand(databaseName, identities, force, raftRequestId));

            if (result == null)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should update a cluster-wide identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            var newIdentitiesResult = result as Dictionary<string, long> ?? throw new InvalidOperationException(
                                 $"Expected to get result from raft command that should update a cluster-wide identity, but got invalid result structure for {id}. Leader is {LeaderTag}, Current node tag is {NodeTag}.");

            if (newIdentitiesResult.TryGetValue(IncrementClusterIdentityCommand.GetStorageKey(databaseName, id), out long newIdentityValue) == false)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should update a cluster-wide identity, but {id} was not in the result list. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            if (newIdentityValue == -1)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should update a cluster-wide identity, but {id} was set but not able to be read. shouldn't reach here. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            return newIdentityValue;
        }

        public async Task<List<long>> GenerateClusterIdentitiesBatchAsync(string databaseName, List<string> ids, string raftRequestId)
        {
            var (_, identityInfoResult) = await SendToLeaderAsync(new IncrementClusterIdentitiesBatchCommand(databaseName, ids, raftRequestId));

            var identityInfo = identityInfoResult as List<long> ?? throw new InvalidOperationException(
                    $"Expected to get result from raft command that should generate a cluster-wide batch identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");

            return identityInfo;
        }

        public NodeInfo GetNodeInfo()
        {
            var memoryInformation = Server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfo);
            var clusterTopology = GetClusterTopology();
            return new NodeInfo
            {
                NodeTag = NodeTag,
                TopologyId = clusterTopology.TopologyId,
                Certificate = Server.Certificate.CertificateForClients,
                NumberOfCores = ProcessorInfo.ProcessorCount,
                InstalledMemoryInGb = memoryInformation.InstalledMemory.GetDoubleValue(SizeUnit.Gigabytes),
                UsableMemoryInGb = memoryInformation.TotalPhysicalMemory.GetDoubleValue(SizeUnit.Gigabytes),
                BuildInfo = LicenseManager.BuildInfo,
                OsInfo = LicenseManager.OsInfo,
                ServerId = GetServerId(),
                CurrentState = CurrentRachisState,
                ServerRole = clusterTopology.GetServerRoleForTag(NodeTag),
                HasFixedPort = HasFixedPort,
                ServerSchemaVersion = SchemaUpgrader.CurrentVersion.ServerVersion
            };
        }

        public License LoadLicense()
        {
            return LoadLicense(ContextPool);
        }

        public License LoadLicense(TransactionContextPool contextPool)
        {
            var lowerName = LicenseStorageKey.ToLowerInvariant();

            using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (Slice.From(context.Allocator, lowerName, out Slice key))
            {
                var licenseBlittable = ClusterStateMachine.ReadInternal(context, out _, key);
                if (licenseBlittable == null)
                    return null;

                return JsonDeserializationServer.License(licenseBlittable);
            }
        }

        public LicenseLimits LoadLicenseLimits()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var licenseLimitsBlittable = Cluster.Read(context, LicenseLimitsStorageKey);
                if (licenseLimitsBlittable == null)
                    return null;

                return JsonDeserializationServer.LicenseLimits(licenseLimitsBlittable);
            }
        }

        public async Task PutLicenseAsync(License license, string raftRequestId, bool fromApi = false)
        {
            var command = new PutLicenseCommand(LicenseStorageKey, license, raftRequestId, fromApi);

            var result = await SendToLeaderAsync(command);

            if (Logger.IsInfoEnabled)
                Logger.Info($"Updating license id: {license.Id}");

            await Cluster.WaitForIndexNotification(result.Index);
        }

        public async Task PutNodeLicenseLimitsAsync(string nodeTag, DetailsPerNode detailsPerNode, LicenseStatus licenseStatus, string raftRequestId = null)
        {
            var nodeLicenseLimits = new NodeLicenseLimits
            {
                NodeTag = nodeTag,
                DetailsPerNode = detailsPerNode,
                LicensedCores = licenseStatus.MaxCores,
                MaxCoresPerNode = licenseStatus.MaxCoresPerNode,
                AllNodes = GetClusterTopology().AllNodes.Keys.ToList()
            };

            var command = new UpdateLicenseLimitsCommand(LicenseLimitsStorageKey, nodeLicenseLimits, raftRequestId ?? RaftIdGenerator.NewId());

            var result = await SendToLeaderAsync(command);

            await Cluster.WaitForIndexNotification(result.Index);
        }

        public DatabaseTopology LoadDatabaseTopology(string databaseName)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return Cluster.ReadDatabaseTopology(context, databaseName);
            }
        }

        public async Task<(long Index, object Result)> SendToLeaderAsync(CommandBase cmd, CancellationToken? token = null)
        {
            token ??= CancellationToken.None;
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token.Value, _shutdownNotification.Token))
            {
                if (cmd.Timeout != null)
                {
                    cts.CancelAfter(cmd.Timeout.Value);
                }

                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    return await SendToLeaderAsyncInternal(context, cmd, cts.Token);
            }
        }

        private async Task<(long Index, object Result)> SendToLeaderAsyncInternal(TransactionOperationContext context, CommandBase cmd, CancellationToken token)
        {
            //I think it is reasonable to expect timeout twice of error retry
            var timeoutTask = TimeoutManager.WaitFor(Engine.OperationTimeout, token);
            Exception requestException = null;
            while (true)
            {
                token.ThrowIfCancellationRequested();

                if (_engine.CurrentState == RachisState.Leader && _engine.CurrentLeader?.Running == true)
                {
                    try
                    {
                        return await _engine.PutAsync(cmd);
                    }
                    catch (Exception e) when (e is ConcurrencyException || e is NotLeadingException)
                    {
                        // if the leader was changed during the PutAsync, we will retry.
                        continue;
                    }
                }
                if (_engine.CurrentState == RachisState.Passive)
                {
                    ThrowInvalidEngineState(cmd);
                }

                var logChange = _engine.WaitForHeartbeat();

                var reachedLeader = new Reference<bool>();
                var cachedLeaderTag = _engine.LeaderTag; // not actually working
                try
                {
                    if (cachedLeaderTag == null)
                    {
                        await Task.WhenAny(logChange, timeoutTask);
                        token.ThrowIfCancellationRequested();

                        if (timeoutTask.IsCompleted)
                            ThrowTimeoutException(cmd, requestException);

                        continue;
                    }

                    var response = await SendToNodeAsync(context, cachedLeaderTag, cmd, reachedLeader, token);
                    return (response.Index, cmd.FromRemote(response.Result));
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Tried to send message to leader (reached: {reachedLeader.Value}), retrying", ex);

                    if (reachedLeader.Value)
                        throw;

                    requestException = ex;
                }

                await Task.WhenAny(logChange, timeoutTask);
                token.ThrowIfCancellationRequested();

                if (timeoutTask.IsCompleted)
                {
                    ThrowTimeoutException(cmd, requestException);
                }
            }
        }

        [DoesNotReturn]
        private static void ThrowInvalidEngineState(CommandBase cmd)
        {
            throw new NotSupportedException("Cannot send command " + cmd.GetType().FullName + " to the cluster because this node is passive." + Environment.NewLine +
                                            "Passive nodes aren't members of a cluster and require admin action (such as creating a db) " +
                                            "to indicate that this node should create its own cluster");
        }

        [DoesNotReturn]
        private void ThrowTimeoutException(CommandBase cmd, Exception requestException)
        {
            throw new TimeoutException($"Could not send command {cmd.GetType().FullName} from {NodeTag} to leader because there is no leader, " +
                                       $"and we timed out waiting for one after {Engine.OperationTimeout}", requestException);
        }

        private async Task<(long Index, object Result)> SendToNodeAsync(TransactionOperationContext context, string engineLeaderTag, CommandBase cmd,
            Reference<bool> reachedLeader, CancellationToken token)
        {
            var djv = cmd.ToJson(context);
            var cmdJson = context.ReadObject(djv, "raft/command");

            ClusterTopology clusterTopology;
            using (context.OpenReadTransaction())
                clusterTopology = _engine.GetTopology(context);

            if (clusterTopology.Members.TryGetValue(engineLeaderTag, out string leaderUrl) == false)
                throw new InvalidOperationException("Leader " + engineLeaderTag + " was not found in the topology members");

            var serverCertificateChanged = Interlocked.Exchange(ref _serverCertificateChanged, 0) == 1;

            if (_leaderRequestExecutor == null
                || serverCertificateChanged
                || _leaderRequestExecutor.Url.Equals(leaderUrl, StringComparison.OrdinalIgnoreCase) == false)
            {
                var newExecutor = CreateNewClusterRequestExecutor(leaderUrl);
                Interlocked.Exchange(ref _leaderRequestExecutor, newExecutor);
            }

            cmdJson.TryGet("Type", out string commandType);
            var command = new PutRaftCommand(_leaderRequestExecutor.Conventions, cmdJson, _engine.Url, commandType)
            {
                Timeout = cmd.Timeout
            };

            try
            {
                await _leaderRequestExecutor.ExecuteAsync(command, context, token: token);
            }
            catch
            {
                reachedLeader.Value = command.HasReachLeader();
                throw;
            }

            return (command.Result.RaftCommandIndex, command.Result.Data);
        }

        protected internal async Task WaitForExecutionOnSpecificNodeAsync(JsonOperationContext context, string node, long index)
        {
            await Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader

            using (var requester = ClusterRequestExecutor.CreateForShortTermUse(GetClusterTopology().GetUrlFromTag(node), Server.Certificate.Certificate, Server.Conventions))
            using (var oct = new OperationCancelToken(cancelAfter: Configuration.Cluster.OperationTimeout.AsTimeSpan, token: ServerShutdown))
                await requester.ExecuteAsync(new WaitForRaftIndexCommand(index), context, token: oct.Token);
        }

        public async Task WaitForExecutionOnRelevantNodesAsync(JsonOperationContext context, List<string> members, long index)
        {
            await Cluster.WaitForIndexNotification(index); // first let see if we commit this in the leader

            if (members == null || members.Count == 0)
                throw new InvalidOperationException("Cannot wait for execution when there are no nodes to execute on.");

            using (var requestExecutor = ClusterRequestExecutor.Create(GetClusterTopology().Members.Values.ToArray(), Server.Certificate.Certificate, Server.Conventions))
            using (var oct = new OperationCancelToken(cancelAfter: Configuration.Cluster.OperationTimeout.AsTimeSpan, token: ServerShutdown))
            {
                List<Exception> exceptions = null;

                var waitingTasks =
                    members.Select(member => WaitForRaftIndexOnNodeAndReturnIfExceptionAsync(requestExecutor, member, index, context, oct.Token));

                foreach (var exception in await Task.WhenAll(waitingTasks))
                {
                    if (exception == null)
                        continue;

                    exceptions ??= new List<Exception>();
                    exceptions.Add(exception.ExtractSingleInnerException());
                }

                HandleExceptions(exceptions);
            }

            return;

            void HandleExceptions(IReadOnlyCollection<Exception> exceptions)
            {
                if (exceptions == null || exceptions.Count == 0)
                    return;

                var allExceptionsAreTimeouts = exceptions.All(exception => exception is OperationCanceledException);
                var aggregateException = new RaftIndexWaitAggregateException(index, exceptions);

                if (allExceptionsAreTimeouts)
                    throw new TimeoutException($"The raft command (number '{index}') took too long to run on the intended nodes.", aggregateException);

                throw aggregateException;
            }
        }

        private async Task<Exception> WaitForRaftIndexOnNodeAndReturnIfExceptionAsync(RequestExecutor executor, string nodeTag, long index, JsonOperationContext context, CancellationToken token)
        {
            try
            {
                var cmd = new WaitForRaftIndexCommand(index, nodeTag);
                await executor.ExecuteAsync(cmd, context, token: token);
                return null;
            }
            catch (RavenException re) when (re.InnerException is HttpRequestException)
            {
                // we want to throw for self-checks
                if (nodeTag == NodeTag)
                    return re;

                // ignore - we are ok when connection with a node cannot be established (test: AddDatabaseOnDisconnectedNode)
                return null;
            }
            catch (Exception e)
            {
                return e;
            }
        }

        internal ClusterRequestExecutor CreateNewClusterRequestExecutor(string leaderUrl)
        {
            var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leaderUrl, Server.Certificate.Certificate, Server.Conventions);
            requestExecutor.DefaultTimeout = Engine.OperationTimeout;

            return requestExecutor;
        }

        private sealed class PutRaftCommand : RavenCommand<PutRaftCommandResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _command;
            private bool _reachedLeader;
            public override bool IsReadRequest => false;

            public bool HasReachLeader() => _reachedLeader;

            private readonly string _source;
            private readonly string _commandType;

            public PutRaftCommand(DocumentConventions conventions, BlittableJsonReaderObject command, string source, string commandType)
            {
                _conventions = conventions;
                _command = command;
                _source = source;
                _commandType = commandType;
            }

            public override void OnResponseFailure(HttpResponseMessage response)
            {
                if (response.Headers.Contains("Reached-Leader") == false)
                    return;
                _reachedLeader = response.Headers.GetValues("Reached-Leader").Contains("true");
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/rachis/send?source={_source}&commandType={_commandType}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteObject(_command);
                        }
                    }, _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationCluster.PutRaftCommandResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public sealed class PutRaftCommandResult
        {
            public long RaftCommandIndex { get; set; }

            public object Data { get; set; }
        }

        public Task WaitForTopology(Leader.TopologyModification state, CancellationToken token)
        {
            return _engine.WaitForTopology(state, token: token);
        }

        public Task<bool> WaitForState(RachisState rachisState, CancellationToken token)
        {
            return _engine.WaitForState(rachisState, token);
        }

        public async Task ClusterAcceptNewConnectionAsync(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header, Action disconnect, EndPoint remoteEndpoint)
        {
            try
            {
                if (_engine == null)
                {
                    // on startup, the tcp listeners are initialized prior to the engine, so there could be a race.
                    disconnect();
                    return;
                }

                var features = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Cluster, tcp.ProtocolVersion);
                var remoteConnection = new RemoteConnection(_engine.Tag, _engine.CurrentTerm, tcp.Stream, features.Cluster, disconnect);

                await _engine.AcceptNewConnectionAsync(remoteConnection, remoteEndpoint);
            }
            catch (IOException e)
            {
                // expected exception on network failures.
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Failed to accept new RAFT connection via TCP from node {header.SourceNodeTag} ({remoteEndpoint}).", e);
                }
            }
            catch (RachisException e)
            {
                // rachis exceptions are expected, so we will not raise an alert, but only log them.
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Failed to accept new RAFT connection via TCP from node {header.SourceNodeTag} ({remoteEndpoint}).", e);
                }
            }
            catch (Exception e)
            {
                var msg = $"Failed to accept new RAFT connection via TCP from {header.SourceNodeTag} ({remoteEndpoint}).";
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info(msg, e);
                }

                NotificationCenter.Add(AlertRaised.Create(Notification.ServerWide, "RAFT connection error", msg,
                    AlertType.ClusterTopologyWarning, NotificationSeverity.Error, key: ((IPEndPoint)remoteEndpoint).Address.ToString(), details: new ExceptionDetails(e)));
            }
        }

        public Task WaitForCommitIndexChange(RachisConsensus.CommitIndexModification modification, long value, TimeSpan timeout, CancellationToken token = default)
        {
            return _engine.WaitForCommitIndexChange(modification, value, timeout, token);
        }

        public Task WaitForCommitIndexChange(RachisConsensus.CommitIndexModification modification, long value, CancellationToken token = default)
        {
            return _engine.WaitForCommitIndexChange(modification, value, timeout: null, token);
        }

        public string LastStateChangeReason()
        {
            return $"{_engine.CurrentState}, {_engine.LastStateChangeReason} (at {_engine.LastStateChangeTime})";
        }

        public string GetNodeHttpServerUrl(string clientRequestedNodeUrl = null)
        {
            Debug.Assert(Configuration.Core.PublicServerUrl.HasValue || _server.WebUrl != null || clientRequestedNodeUrl != null);

            return Configuration.Core.GetNodeHttpServerUrl(
                (Configuration.Core.PublicServerUrl?.UriValue ?? clientRequestedNodeUrl) ?? _server.WebUrl
            );
        }

        public string GetNodeTcpServerUrl(string clientRequestedNodeUrl = null)
        {
            var ravenServerWebUrl = clientRequestedNodeUrl ?? _server.WebUrl;
            if (ravenServerWebUrl == null)
                ThrowInvalidTcpUrlOnStartup();
            var status = _server.GetTcpServerStatus();
            return Configuration.Core.GetNodeTcpServerUrl(ravenServerWebUrl, status.Port);
        }

        public string[] GetNodeClusterTcpServerUrls(string clientRequestedNodeUrl = null, bool forExternalUse = false)
        {
            UriSetting[] urls = forExternalUse ? Configuration.Core.ExternalPublicTcpServerUrl : Configuration.Core.ClusterPublicTcpServerUrl;
            if (urls == null)
                return new[] { GetNodeTcpServerUrl(clientRequestedNodeUrl) };
            var length = urls.Length;
            var res = new string[length];
            for (var i = 0; i < length; i++)
            {
                res[i] = urls[i].UriValue;
            }
            return res;
        }

        public async Task<NodeConnectionTestResult> TestConnectionFromRemote(RequestExecutor requestExecutor, JsonOperationContext context, string nodeUrl)
        {
            var myUrl = GetNodeHttpServerUrl();
            NodeConnectionTestResult result;

            var nodeConnectionTest = new TestNodeConnectionCommand(myUrl);
            try
            {
                await requestExecutor.ExecuteAsync(nodeConnectionTest, context);
                result = nodeConnectionTest.Result;

                if (nodeConnectionTest.Result.Success == false)
                {
                    result.Success = false;
                    result.Error = $"{NodeConnectionTestResult.GetError(myUrl, nodeUrl)}{Environment.NewLine}{nodeConnectionTest.Result.Error}";
                }
            }
            catch (Exception e)
            {
                return new NodeConnectionTestResult
                {
                    Success = false,
                    Error = $"{NodeConnectionTestResult.GetError(myUrl, nodeUrl)}{Environment.NewLine}{e}"
                };
            }

            return result;
        }

        public async Task<NodeConnectionTestResult> TestConnectionToRemote(string url, string database)
        {
            Task<TcpConnectionInfo> connectionInfo;
            try
            {
                var timeout = TimeoutManager.WaitFor(Configuration.Cluster.OperationTimeout.AsTimeSpan);

                using (var cts = new CancellationTokenSource(Server.Configuration.Cluster.OperationTimeout.AsTimeSpan))
                {
                    connectionInfo = ReplicationUtils.GetDatabaseTcpInfoAsync(GetNodeHttpServerUrl(), url, database, "Test-Connection", Server.Certificate.Certificate,
                        cts.Token);
                }
                Task timeoutTask = await Task.WhenAny(timeout, connectionInfo);
                if (timeoutTask == timeout)
                {
                    throw new TimeoutException($"Waited for {Configuration.Cluster.OperationTimeout.AsTimeSpan} to receive TCP information from '{url}' and got no response");
                }
                await connectionInfo;
            }
            catch (Exception e)
            {
                return new NodeConnectionTestResult
                {
                    Success = false,
                    HTTPSuccess = false,
                    Error = $"An exception was thrown while trying to connect to '{url}':{Environment.NewLine}{e}"
                };
            }

            var result = new NodeConnectionTestResult
            {
                HTTPSuccess = true,
                TcpServerUrl = connectionInfo.Result.Url
            };

            try
            {
                await TestConnectionHandler.ConnectToClientNodeAsync(_server, connectionInfo.Result, Engine.TcpConnectionTimeout,
                    LoggingSource.Instance.GetLogger("testing-connection", "testing-connection"), database, result, ServerShutdown);
            }
            catch (Exception e)
            {
                result.Success = false;
                result.Error = $"Was able to connect to url '{url}', but exception was thrown while trying to connect to TCP port '{connectionInfo.Result.Url}':{Environment.NewLine}{e}";
            }

            return result;
        }

        [DoesNotReturn]
        private static void ThrowInvalidTcpUrlOnStartup()
        {
            throw new InvalidOperationException("The server has yet to complete startup, cannot get NodeTcpServerUtl");
        }

        public DynamicJsonValue GetTcpInfoAndCertificates(string clientRequestedNodeUrl, bool forExternalUse = false)
        {
            var tcpServerUrl = GetNodeTcpServerUrl(clientRequestedNodeUrl);
            if (tcpServerUrl.StartsWith("tcp://localhost.fiddler:", StringComparison.OrdinalIgnoreCase))
                tcpServerUrl = tcpServerUrl.Remove(15, 8);

            var res = new DynamicJsonValue
            {
                [nameof(TcpConnectionInfo.Url)] = tcpServerUrl,
                [nameof(TcpConnectionInfo.Certificate)] = _server.Certificate.CertificateForClients,
                [nameof(TcpConnectionInfo.NodeTag)] = NodeTag,
                [nameof(TcpConnectionInfo.ServerId)] = ServerId.ToString()
            };

            var urls = GetNodeClusterTcpServerUrls(clientRequestedNodeUrl, forExternalUse);
            var array = new DynamicJsonArray();
            foreach (var url in urls)
            {
                array.Add(url);
            }

            res[nameof(TcpConnectionInfo.Urls)] = array;

            return res;
        }

        public DynamicJsonValue GetLogDetails(ClusterOperationContext context, int max = 100)
        {
            RachisConsensus.GetLastTruncated(context, out var index, out var term);
            var range = Engine.GetLogEntriesRange(context);
            var entries = new DynamicJsonArray();
            foreach (var entry in Engine.GetLogEntries(range.Min, context, max))
            {
                entries.Add(entry.ToString());
            }

            var json = new DynamicJsonValue
            {
                [nameof(LogSummary.CommitIndex)] = Engine.GetLastCommitIndex(context),
                [nameof(LogSummary.LastTruncatedIndex)] = index,
                [nameof(LogSummary.LastTruncatedTerm)] = term,
                [nameof(LogSummary.FirstEntryIndex)] = range.Min,
                [nameof(LogSummary.LastLogEntryIndex)] = range.Max,
                [nameof(LogSummary.Entries)] = entries
            };
            return json;
        }

        public IEnumerable<Client.ServerWide.Operations.MountPointUsage> GetMountPointUsageDetailsFor(StorageEnvironmentWithType environment, bool includeTempBuffers)
        {
            var fullPath = environment?.Environment.Options.BasePath.FullPath;
            if (fullPath == null)
                yield break;

            var driveInfo = environment.Environment.Options.DriveInfoByPath?.Value;
            var diskSpaceResult = DiskUtils.GetDiskSpaceInfo(fullPath, driveInfo?.BasePath);
            if (diskSpaceResult == null)
                yield break;

            var sizeOnDisk = environment.Environment.GenerateSizeReport(includeTempBuffers);
            var usage = new Client.ServerWide.Operations.MountPointUsage
            {
                Name = environment.Name,
                Type = environment.Type.ToString(),
                UsedSpace = sizeOnDisk.DataFileInBytes,
                DiskSpaceResult = FillDiskSpaceResult(diskSpaceResult),
                UsedSpaceByTempBuffers = 0
            };

            var ioStatsResult = Server.DiskStatsGetter.Get(driveInfo?.BasePath.DriveName);
            if (ioStatsResult != null)
                usage.IoStatsResult = FillIoStatsResult(ioStatsResult);

            if (diskSpaceResult.DriveName == driveInfo?.JournalPath.DriveName)
            {
                usage.UsedSpace += sizeOnDisk.JournalsInBytes;
                usage.UsedSpaceByTempBuffers += includeTempBuffers ? sizeOnDisk.TempRecyclableJournalsInBytes : 0;
            }
            else
            {
                var journalDiskSpaceResult = DiskUtils.GetDiskSpaceInfo(environment.Environment.Options.JournalPath?.FullPath, driveInfo?.JournalPath);
                if (journalDiskSpaceResult != null)
                {
                    var journalUsage = new Client.ServerWide.Operations.MountPointUsage
                    {
                        Name = environment.Name,
                        Type = environment.Type.ToString(),
                        DiskSpaceResult = FillDiskSpaceResult(journalDiskSpaceResult),
                        UsedSpaceByTempBuffers = includeTempBuffers ? sizeOnDisk.TempRecyclableJournalsInBytes : 0
                    };
                    var journalIoStatsResult = Server.DiskStatsGetter.Get(driveInfo?.JournalPath.DriveName);
                    if (journalIoStatsResult != null)
                        usage.IoStatsResult = FillIoStatsResult(ioStatsResult);

                    yield return journalUsage;
                }
            }

            if (includeTempBuffers)
            {
                if (diskSpaceResult.DriveName == driveInfo?.TempPath.DriveName)
                {
                    usage.UsedSpaceByTempBuffers += sizeOnDisk.TempBuffersInBytes;
                }
                else
                {
                    var tempBuffersDiskSpaceResult = DiskUtils.GetDiskSpaceInfo(environment.Environment.Options.TempPath.FullPath, driveInfo?.TempPath);
                    if (tempBuffersDiskSpaceResult != null)
                    {
                        var tempBuffersUsage = new Client.ServerWide.Operations.MountPointUsage
                        {
                            Name = environment.Name,
                            Type = environment.Type.ToString(),
                            UsedSpaceByTempBuffers = sizeOnDisk.TempBuffersInBytes,
                            DiskSpaceResult = FillDiskSpaceResult(tempBuffersDiskSpaceResult)
                        };
                        var tempBufferIoStatsResult = Server.DiskStatsGetter.Get(driveInfo?.TempPath.DriveName);
                        if (tempBufferIoStatsResult != null)
                            tempBuffersUsage.IoStatsResult = FillIoStatsResult(ioStatsResult);

                        yield return tempBuffersUsage;
                    }
                }
            }

            yield return usage;
        }

        private static Client.ServerWide.Operations.DiskSpaceResult FillDiskSpaceResult(Sparrow.Server.Utils.DiskSpaceResult journalDiskSpaceResult)
        {
            return new Client.ServerWide.Operations.DiskSpaceResult
            {
                DriveName = journalDiskSpaceResult.DriveName,
                VolumeLabel = journalDiskSpaceResult.VolumeLabel,
                TotalFreeSpaceInBytes = journalDiskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Bytes),
                TotalSizeInBytes = journalDiskSpaceResult.TotalSize.GetValue(SizeUnit.Bytes)
            };
        }

        internal static IoStatsResult FillIoStatsResult(DiskStatsResult ioStatsResult)
        {
            return new IoStatsResult
            {
                IoReadOperations = ioStatsResult.IoReadOperations,
                IoWriteOperations = ioStatsResult.IoWriteOperations,
                ReadThroughputInKb = ioStatsResult.ReadThroughput.GetValue(SizeUnit.Kilobytes),
                WriteThroughputInKb = ioStatsResult.WriteThroughput.GetValue(SizeUnit.Kilobytes),
                QueueLength = ioStatsResult.QueueLength,
            };
        }

        public StreamsTempFile GetTempFile(string fileTypeOrName, string suffix, bool? isEncrypted = null)
        {
            var name = $"{fileTypeOrName}.{Guid.NewGuid():N}.{suffix}";
            var tempPath = _env.Options.DataPager.Options.TempPath.Combine(name);

            return new StreamsTempFile(tempPath.FullPath, isEncrypted ?? _env.Options.Encryption.IsEnabled);
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action BeforePutLicenseCommandHandledInOnValueChanged;
            internal bool StopIndex;
            internal Action<CompareExchangeCommandBase> ModifyCompareExchangeTimeout;
            internal Action RestoreDatabaseAfterSavingDatabaseRecord;
            internal Action AfterCommitInClusterTransaction;
            internal Action<string, List<ClusterTransactionCommand.SingleClusterDatabaseCommand>> BeforeExecuteClusterTransactionBatch;
        }

#if DEBUG
        public bool EnableCaptureWriteTransactionStackTrace = false;
#endif

        public readonly MemoryCache QueryClauseCache;

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;
            // just discard the whole thing
            QueryClauseCache.Clear();
        }

        public void LowMemoryOver()
        {
        }
    }
}
