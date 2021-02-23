using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Server;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Dashboard;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Storage;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
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
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server-wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private const string ResourceName = nameof(ServerStore);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStore>(ResourceName);

        public const string LicenseStorageKey = "License/Key";

        public const string LicenseLimitsStorageKey = "License/Limits/Key";

        private readonly CancellationTokenSource _shutdownNotification = new CancellationTokenSource();

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        internal StorageEnvironment _env;

        internal readonly SizeLimitedConcurrentDictionary<string, ConcurrentQueue<DateTime>> ClientCreationRate = 
            new SizeLimitedConcurrentDictionary<string, ConcurrentQueue<DateTime>>(50);

        private readonly NotificationsStorage _notificationsStorage;
        private readonly OperationsStorage _operationsStorage;
        public ConcurrentDictionary<string, Dictionary<string, long>> IdleDatabases;

        private RequestExecutor _clusterRequestExecutor;
        private long _lastClusterTopologyIndex = -1;

        public readonly RavenConfiguration Configuration;
        private readonly RavenServer _server;
        public readonly DatabasesLandlord DatabasesLandlord;
        public readonly NotificationCenter.NotificationCenter NotificationCenter;
        public readonly ServerDashboardNotifications ServerDashboardNotifications;
        public readonly LicenseManager LicenseManager;
        public readonly FeedbackSender FeedbackSender;
        public readonly StorageSpaceMonitor StorageSpaceMonitor;
        public readonly SecretProtection Secrets;
        public readonly AsyncManualResetEvent InitializationCompleted;
        public readonly GlobalIndexingScratchSpaceMonitor GlobalIndexingScratchSpaceMonitor;
        public bool Initialized;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        public long LastClientConfigurationIndex { get; private set; } = -2;

        public ConcurrentBackupsCounter ConcurrentBackupsCounter { get; private set; }

        public Operations Operations { get; }

        public ServerStore(RavenConfiguration configuration, RavenServer server)
        {
            // we want our servers to be robust get early errors about such issues
            MemoryInformation.EnableEarlyOutOfMemoryChecks = true;

            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            _server = server;

            IdleDatabases = new ConcurrentDictionary<string, Dictionary<string, long>>(StringComparer.OrdinalIgnoreCase);

            DatabasesLandlord = new DatabasesLandlord(this);

            _notificationsStorage = new NotificationsStorage(ResourceName);

            NotificationCenter = new NotificationCenter.NotificationCenter(_notificationsStorage, null, ServerShutdown, configuration);

            ServerDashboardNotifications = new ServerDashboardNotifications(this, ServerShutdown);

            _operationsStorage = new OperationsStorage();

            Operations = new Operations(null, _operationsStorage, NotificationCenter, null,
                (PlatformDetails.Is32Bits || Configuration.Storage.ForceUsing32BitsPager
                        ? TimeSpan.FromHours(12)
                        : TimeSpan.FromDays(2)));

            LicenseManager = new LicenseManager(this);

            FeedbackSender = new FeedbackSender();

            StorageSpaceMonitor = new StorageSpaceMonitor(NotificationCenter);

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
        }

        private void OnServerCertificateChanged(object sender, EventArgs e)
        {
            Interlocked.Exchange(ref _serverCertificateChanged, 1);
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
                                if (leader == null || leader == _engine.Tag)
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

                                        var readTask = context.ReadFromWebSocket(ws, "ws from Leader", cts.Token);
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
                    using (Observer = new ClusterObserver(this, ClusterMaintenanceSupervisor, _engine, term, ContextPool, ServerShutdown))
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
        public bool HasTopologyChanged(long topologyEtag)
        {
            return _lastClusterTopologyIndex != topologyEtag;
        }

        public ClusterTopology GetClusterTopology(TransactionOperationContext context)
        {
            return _engine.GetTopology(context);
        }

        public bool HasFixedPort { get; internal set; }

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

        public void Initialize()
        {
            Configuration.CheckDirectoryPermissions();

            LowMemoryNotification.Instance.Initialize(
                Configuration.Memory.LowMemoryLimit,
                Configuration.Memory.UseTotalDirtyMemInsteadOfMemUsage,
                Configuration.Memory.EnableHighTemporaryDirtyMemoryUse,
                Configuration.Memory.TemporaryDirtyMemoryAllowedPercentage,
                new LowMemoryMonitor(), ServerShutdown);

            MemoryInformation.SetFreeCommittedMemory(
                Configuration.Memory.MinimumFreeCommittedMemoryPercentage,
                Configuration.Memory.MaxFreeCommittedMemoryToKeep,
                Configuration.Memory.LowMemoryCommitLimit);

            if (Logger.IsInfoEnabled)
                Logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

            var path = Configuration.Core.DataDirectory.Combine("System");
            var storeAlertForLateRaise = new List<AlertRaised>();

            IoChanges = new IoChangesNotifications();

            StorageEnvironmentOptions options;
            if (Configuration.Core.RunInMemory)
            {
                options = StorageEnvironmentOptions.CreateMemoryOnly();
            }
            else
            {
                options = StorageEnvironmentOptions.ForPath(path.FullPath, null, null, IoChanges, null);
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
                var alert = AlertRaised.Create(
                    null,
                    "Non Durable File System - System Storage",
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
                var alert = AlertRaised.Create(
                    null,
                    "Recovery Error - System Storage",
                    e.Message,
                    AlertType.RecoveryError,
                    NotificationSeverity.Error,
                    "Recovery Error System");

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
                var alert = AlertRaised.Create(
                    null,
                    "Integrity error of already synced data - System Storage",
                    e.Message,
                    AlertType.IntegrityErrorOfAlreadySyncedData,
                    NotificationSeverity.Warning,
                    "Integrity Error of Synced Data - System");

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
                EntityToBlittable.ConvertCommandToBlittable(new DatabaseRecord(), ctx);
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

            _engine = new RachisConsensus<ClusterStateMachine>(this);
            _engine.BeforeAppendToRaftLog = BeforeAppendToRaftLog;

            var myUrl = GetNodeHttpServerUrl();
            _engine.Initialize(_env, Configuration, myUrl, out _lastClusterTopologyIndex);

            LicenseManager.Initialize(_env, ContextPool);
            LatestVersionCheck.Instance.Check(this);

            ConcurrentBackupsCounter = new ConcurrentBackupsCounter(Configuration.Backup, LicenseManager);

            ConfigureAuditLog();

            Initialized = true;
            InitializationCompleted.Set();
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
                    NotificationCenter.Add(AlertRaised.Create(null,
                        "Low swap size",
                        $"The current swap size is '{swapSize}' and it is lower then the threshold defined '{Configuration.PerformanceHints.MinSwapSize}'",
                        AlertType.LowSwapSize,
                        NotificationSeverity.Warning));
                return;
            }

            if(PlatformDetails.RunningOnPosix == false)
            {
                var memoryInfo = MemoryInformation.GetMemoryInfo();
                if(memoryInfo.TotalCommittableMemory - memoryInfo.TotalPhysicalMemory <= Sparrow.Size.Zero)
                    NotificationCenter.Add(AlertRaised.Create(null,
                        "No PageFile available",
                        "Your system has no PageFile. It is recommended to have a PageFile in order for Server to work properly",
                        AlertType.LowSwapSize,
                        NotificationSeverity.Warning));
            }
        }

        private void BeforeAppendToRaftLog(TransactionOperationContext ctx, CommandBase cmd)
        {
            switch (cmd)
            {
                case AddDatabaseCommand addDatabase:
                    if (addDatabase.Record.Topology.Count == 0)
                    {
                        AssignNodesToDatabase(GetClusterTopology(ctx), addDatabase.Record);
                    }
                    Debug.Assert(addDatabase.Record.Topology.Count != 0, "Empty topology after AssignNodesToDatabase");
                    break;
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
            _engine.StateMachine.DatabaseChanged += DatabasesLandlord.ClusterOnDatabaseChanged;
            _engine.StateMachine.DatabaseChanged += OnDatabaseChanged;
            _engine.StateMachine.ValueChanged += OnValueChanged;

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
                                await DatabasesLandlord.ClusterOnDatabaseChanged(db, 0, "Init", DatabasesLandlord.ClusterDatabaseChangeType.RecordChanged);
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
                ClusterMaintenanceSetupTask(), null, "Cluster Maintenance Setup Task");

            _updateTopologyChangeNotification = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
            {
                Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                UpdateTopologyChangeNotification();
            }, null, "Update Topology Change Notification Task");
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
                var completedTask = await Task.WhenAny(tasks.Values);
                var name = tasks.Single(t => t.Value == completedTask).Key;
                tasks.Remove(name);
                try
                {
                    var database = await completedTask;
                    database.RefreshFeatures();
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
                            OnTopologyChangeInternal(clusterTopology);
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

                            OnTopologyChangeInternal(response.Topology, response.Status);
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
                            await Logger.InfoAsync("Unable to notify about cluster topology change", e);
                        }
                    }
                }
            }, ServerShutdown);
        }

        private void OnTopologyChangeInternal(ClusterTopology topology, Dictionary<string, NodeStatus> status = null)
        {
            if (_lastClusterTopologyIndex < topology.Etag)
                _lastClusterTopologyIndex = topology.Etag;

            NotificationCenter.Add(ClusterTopologyChanged.Create(topology, LeaderTag,
                NodeTag, _engine.CurrentTerm, _engine.CurrentState, status ?? GetNodesStatuses(), LoadLicenseLimits()?.NodeLicenseDetails));

            foreach (var db in DatabasesLandlord.DatabasesCache)
            {
                DocumentDatabase dbActual;
                try
                {
                    if (db.Value.IsCompletedSuccessfully == false)
                        continue;
                    dbActual = db.Value.Result;
                }
                catch (Exception)
                {
                    continue;
                }
                dbActual.Changes.RaiseNotifications(new TopologyChange
                {
                    Url = topology.GetUrlFromTag(NodeTag),
                    Database = dbActual.Name
                });
            }
        }

        private Task OnDatabaseChanged(string databaseName, long index, string type, DatabasesLandlord.ClusterDatabaseChangeType _)
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
                    NotificationCenter.Add(DatabaseChanged.Create(databaseName, DatabaseChangeType.Update));
                    break;
                case nameof(RemoveNodeFromDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(databaseName, DatabaseChangeType.RemoveNode));
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
                    // reload license can send a notification which will open a write tx
                    LicenseManager.ReloadLicense();
                    ConcurrentBackupsCounter.ModifyMaxConcurrentBackups();

                    // we are not waiting here on purpose
                    var t = LicenseManager.PutMyNodeInfoAsync().IgnoreUnobservedExceptions();
                    break;
                case nameof(PutLicenseLimitsCommand):
                case nameof(UpdateLicenseLimitsCommand):
                    LicenseManager.ReloadLicenseLimits();
                    ConcurrentBackupsCounter.ModifyMaxConcurrentBackups();
                    NotifyAboutClusterTopologyAndConnectivityChanges();

                    break;
                case nameof(PutServerWideBackupConfigurationCommand):
                    RescheduleTimerForIdleDatabases(index);
                    break;
            }
        }

        private void RescheduleTimerForIdleDatabases(long taskId)
        {
            if (IdleDatabases.IsEmpty)
                return;

            foreach (var db in IdleDatabases.Keys)
            {
                PeriodicBackupConfiguration backupConfig;
                DatabaseTopology topology;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var rawRecord = Cluster.ReadRawDatabaseRecord(ctx, db))
                {
                    topology = rawRecord.Topology;
                    backupConfig = rawRecord.GetPeriodicBackupConfiguration(taskId);

                    if (backupConfig == null)
                        throw new InvalidOperationException($"Could not reschedule the wakeup timer for idle database '{db}', because there is no backup task with id '{taskId}'.");
                }

                var tag = topology.WhoseTaskIsIt(Engine.CurrentState, backupConfig, null);
                if (Engine.Tag != tag)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Could not reschedule the wakeup timer for idle database '{db}', because backup task '{backupConfig.Name}' with id '{taskId}' belongs to node '{tag}' current node is '{Engine.Tag}'.");
                    continue;
                }

                if (backupConfig.Disabled || backupConfig.FullBackupFrequency == null && backupConfig.IncrementalBackupFrequency == null)
                    continue;

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

                var dueTime = (int)(wakeup - now).TotalMilliseconds;
                DatabasesLandlord.RescheduleDatabaseWakeup(db, dueTime, wakeup);

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Rescheduling the wakeup timer for idle database '{db}', because backup task '{backupConfig.Name}' with id '{taskId}' which belongs to node '{Engine.Tag}', new timer is set to: '{wakeup}', with dueTime: {dueTime} ms.");
            }
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
                            Cluster.DeleteItem(context, CertificateReplacement.CertificateReplacementDoc);
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
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cert = Cluster.GetItem(context, CertificateReplacement.CertificateReplacementDoc);
                    if (cert == null)
                        return;
                    if (cert.TryGet(nameof(CertificateReplacement.Thumbprint), out string certThumbprint) == false)
                        throw new InvalidOperationException($"Invalid 'server/cert' value, expected to get '{nameof(CertificateReplacement.Thumbprint)}' property");

                    if (cert.TryGet(nameof(CertificateReplacement.Certificate), out string base64Cert) == false)
                        throw new InvalidOperationException($"Invalid 'server/cert' value, expected to get '{nameof(CertificateReplacement.Certificate)}' property");

                    var certificate = new X509Certificate2(Convert.FromBase64String(base64Cert), (string)null, X509KeyStorageFlags.MachineKeySet);

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

                    // we got it, now let us let the leader know about it
                    await SendToLeaderAsync(new ConfirmReceiptServerCertificateCommand(certThumbprint));
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
                    var newClusterCertificate = new X509Certificate2(bytesToSave, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

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
                            using (var certStream = File.Create(certPath))
                            {
                                certStream.Write(bytesToSave, 0, bytesToSave.Length);
                                certStream.Flush(true);
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
                            Secrets.NotifyExecutableOfCertificateChange(Configuration.Security.CertificateChangeExec,
                                Configuration.Security.CertificateChangeExecArguments,
                                certBase64,
                                this);
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

                    if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion < commandVersion)
                    {
                        // If some nodes run the old version of the command, this node (newer version) will finish here and delete 'server/cert'
                        // because the last stage of the new version (ConfirmServerCertificateReplacedCommand where we delete 'server/cert') will not happen 
                        using (var tx = context.OpenWriteTransaction())
                        {
                            Cluster.DeleteItem(context, CertificateReplacement.CertificateReplacementDoc);
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
                if (rawRecord != null && rawRecord.IsEncrypted== false)
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

        public void DeleteSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            using (var rawRecord = Cluster.ReadRawDatabaseRecord(context, name))
            {
                if (CanDeleteSecretKey() == false)
                {
                    throw new InvalidOperationException(
                        $"Can't delete secret key for a database ({name}) that is relevant for this node ({NodeTag}), please delete the database before deleting the secret key.");
                }

                bool CanDeleteSecretKey()
                {
                    if (rawRecord == null)
                        return true;

                    if (rawRecord.IsEncrypted== false)
                        return true;

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
                PullReplicationAsSink = pullReplicationAsSink
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

        public Task<(long Index, object Result)> ToggleTaskState(long taskId, string taskName, OngoingTaskType type, bool disable, string dbName, string raftRequestId)
        {
            CommandBase disableEnableCommand;
            switch (type)
            {
                case OngoingTaskType.Subscription:
                    if (taskName == null)
                    {
                        if (_server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var databaseTask) == false)
                            throw new DatabaseDoesNotExistException($"Can't get subscription name because The database {dbName} does not exists");
                        using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            taskName = databaseTask.Result.SubscriptionStorage.GetSubscriptionNameById(ctx, taskId);
                        }
                    }
                    disableEnableCommand = new ToggleSubscriptionStateCommand(taskName, disable, dbName, raftRequestId);
                    break;
                default:
                    disableEnableCommand = new ToggleTaskStateCommand(taskId, type, disable, dbName, raftRequestId);
                    break;
            }
            return SendToLeaderAsync(disableEnableCommand);
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

        public Task<(long Index, object Result)> DeleteValueInClusterAsync(string key)
        {
            var deleteValueCommand = new DeleteValueCommand
            {
                Name = key
            };
            return SendToLeaderAsync(deleteValueCommand);
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

        public Task<(long Index, object Result)> DeleteServerWideBackupConfigurationAsync(string configurationName, string raftRequestId)
        {
            var command = new DeleteServerWideBackupConfigurationCommand(configurationName, raftRequestId);

            return SendToLeaderAsync(command);
        }

        public async Task<(long, object)> ModifyPeriodicBackup(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var modifyPeriodicBackup = new UpdatePeriodicBackupCommand(JsonDeserializationCluster.PeriodicBackupConfiguration(configurationJson), name, raftRequestId);
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
                    default:
                        throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
                }
            }

            return await SendToLeaderAsync(command);
        }

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

        private bool ValidateConnectionString(RawDatabaseRecord databaseRecord, string connectionStringName, EtlType etlType)
        {
            if (etlType == EtlType.Raven)
            {
                var ravenConnectionStrings = databaseRecord.RavenConnectionStrings;
                return ravenConnectionStrings != null && ravenConnectionStrings.TryGetValue(connectionStringName, out _);
            }

            var sqlConnectionString = databaseRecord.SqlConnectionStrings;
            return sqlConnectionString != null && sqlConnectionString.TryGetValue(connectionStringName, out _);
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
            if (connectionString.TryGet(nameof(ConnectionString.Type), out string type) == false)
                throw new InvalidOperationException($"Connection string must have {nameof(ConnectionString.Type)} field");

            if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

            UpdateDatabaseCommand command;

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    command = new PutRavenConnectionStringCommand(JsonDeserializationCluster.RavenConnectionString(connectionString), databaseName, raftRequestId);
                    break;
                case ConnectionStringType.Sql:
                    command = new PutSqlConnectionStringCommand(JsonDeserializationCluster.SqlConnectionString(connectionString), databaseName, raftRequestId);
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

                    _server.Statistics.Persist(ContextPool, Logger);

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
                        NotificationCenter,
                        LicenseManager,
                        DatabasesLandlord,
                        _env,
                        _clusterRequestExecutor,
                        ContextPool,
                        ByteStringMemoryCache.Cleaner,
                        InitializationCompleted
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

                    var maxTimeDatabaseCanBeIdle = Configuration.Databases.MaxIdleTime.AsTimeSpan;

                    var databasesToCleanup = DatabasesLandlord.LastRecentlyUsed.ForceEnumerateInThreadSafeManner()
                        .Where(x => SystemTime.UtcNow - x.Value > maxTimeDatabaseCanBeIdle)
                        .Select(x => x.Key)
                        .ToArray();

                    foreach (var db in databasesToCleanup)
                    {

                        if (DatabasesLandlord.DatabasesCache.TryGetValue(db, out Task<DocumentDatabase> resourceTask) == false ||
                            resourceTask == null ||
                            resourceTask.Status != TaskStatus.RanToCompletion)
                        {
                            continue;
                        }

                        var idleDbInstance = resourceTask.Result;

                        // intentionally inside the loop, so we get better concurrency overall
                        // since shutting down a database can take a while
                        if (idleDbInstance.Configuration.Core.RunInMemory)
                            continue;

                        if (idleDbInstance.CanUnload == false)
                            continue;

                        if (SystemTime.UtcNow - DatabasesLandlord.LastWork(idleDbInstance) < maxTimeDatabaseCanBeIdle)
                            continue;

                        if (idleDbInstance.Changes.Connections.Values.Any(x => x.IsDisposed == false && x.IsChangesConnectionOriginatedFromStudio == false))
                            continue;

                        if (idleDbInstance.Operations.HasActive)
                            continue;

                        var dbIdEtagDictionary = new Dictionary<string, long>();
                        using (idleDbInstance.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                        using (documentsContext.OpenReadTransaction())
                        {
                            foreach (var kvp in DocumentsStorage.GetAllReplicatedEtags(documentsContext))
                            {
                                dbIdEtagDictionary[kvp.Key] = kvp.Value;
                            }
                        }

                        if (DatabasesLandlord.UnloadDirectly(db, idleDbInstance.PeriodicBackupRunner.GetWakeDatabaseTimeUtc(idleDbInstance.Name)))
                            IdleDatabases[idleDbInstance.Name] = dbIdEtagDictionary;
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

        private static bool DatabaseNeedsToRunIdleOperations(DocumentDatabase database, out CleanupMode mode)
        {
            var now = DateTime.UtcNow;

            var envs = database.GetAllStoragesEnvironment();

            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            if ((now - maxLastWork).TotalMinutes > 5)
            {
                mode = CleanupMode.Deep;
                return true;
            }

            if ((now - database.LastIdleTime).TotalMinutes > 10)
            {
                mode = CleanupMode.Regular;
                return true;
            }

            mode = CleanupMode.None;
            return false;
        }

        public void AssignNodesToDatabase(ClusterTopology clusterTopology, DatabaseRecord record)
        {
            var topology = record.Topology;

            Debug.Assert(topology != null);

            if (clusterTopology.AllNodes.Count == 0)
                throw new InvalidOperationException($"Database {record.DatabaseName} cannot be created, because the cluster topology is empty (shouldn't happen)!");

            if (record.Topology.ReplicationFactor == 0)
                throw new InvalidOperationException($"Database {record.DatabaseName} cannot be created with replication factor of 0.");

            var clusterNodes = clusterTopology.Members.Keys
                .Concat(clusterTopology.Watchers.Keys)
                .ToList();

            if (record.Encrypted)
            {
                clusterNodes.RemoveAll(n => AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(n)));
                if (clusterNodes.Count < topology.ReplicationFactor)
                    throw new InvalidOperationException(
                        $"Database {record.DatabaseName} is encrypted and requires {topology.ReplicationFactor} node(s) which supports SSL. There are {clusterNodes.Count} such node(s) available in the cluster.");
            }

            if (clusterNodes.Count < topology.ReplicationFactor)
            {
                throw new InvalidOperationException(
                    $"Database {record.DatabaseName} requires {topology.ReplicationFactor} node(s) but there are {clusterNodes.Count} nodes available in the cluster.");
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
            var factor = topology.ReplicationFactor;
            var count = Math.Min(clusterNodes.Count, factor);
            for (var i = 0; i < count; i++)
            {
                factor--;
                var selectedNode = clusterNodes[(i + offset) % clusterNodes.Count];
                topology.Members.Add(selectedNode);
            }

            // only if all the online nodes are occupied, try to place on the disconnected
            for (int i = 0; i < Math.Min(disconnectedNodes.Count, factor); i++)
            {
                var selectedNode = disconnectedNodes[(i + offset) % disconnectedNodes.Count];
                topology.Members.Add(selectedNode);
            }
        }

        public Task<(long Index, object Result)> WriteDatabaseRecordAsync(
            string databaseName, DatabaseRecord record, long? index, string raftRequestId,
            Dictionary<string, BlittableJsonReaderObject> databaseValues = null, bool isRestore = false)
        {
            databaseValues ??= new Dictionary<string, BlittableJsonReaderObject>();

            Debug.Assert(record.Topology != null);

            if (string.IsNullOrEmpty(record.Topology.DatabaseTopologyIdBase64))
                record.Topology.DatabaseTopologyIdBase64 = Guid.NewGuid().ToBase64Unpadded();

            record.Topology.Stamp ??= new LeaderStamp();
            record.Topology.Stamp.Term = _engine.CurrentTerm;
            record.Topology.Stamp.LeadersTicks = _engine.CurrentLeader?.LeaderShipDuration ?? 0;

            var addDatabaseCommand = new AddDatabaseCommand(raftRequestId)
            {
                Name = databaseName,
                RaftCommandIndex = index,
                Record = record,
                DatabaseValues = databaseValues,
                IsRestore = isRestore
            };

            return SendToLeaderAsync(addDatabaseCommand);
        }

        public void EnsureNotPassive(string publicServerUrl = null, string nodeTag = "A", bool skipLicenseActivation = false)
        {
            if (_engine.CurrentState != RachisState.Passive)
                return;

            if (_engine.Bootstrap(publicServerUrl ?? _server.ServerStore.GetNodeHttpServerUrl(), nodeTag) == false)
                return;

            if (skipLicenseActivation == false)
                LicenseManager.TryActivateLicense(Server.ThrowOnLicenseActivationFailure);

            // we put a certificate in the local state to tell the server who to trust, and this is done before
            // the cluster exists (otherwise the server won't be able to receive initial requests). Only when we 
            // create the cluster, we register those local certificates in the cluster.
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                using (ctx.OpenReadTransaction())
                {
                    foreach (var localCertKey in Cluster.GetCertificateThumbprintsFromLocalState(ctx))
                    {
                        // if there are trusted certificates in the local state, we will register them in the cluster now
                        using (var localCertificate = Cluster.GetLocalStateByThumbprint(ctx, localCertKey))
                        {
                            var certificateDefinition = JsonDeserializationServer.CertificateDefinition(localCertificate);
                            PutValueInClusterAsync(new PutCertificateCommand(localCertKey, certificateDefinition, RaftIdGenerator.NewId())).Wait(ServerShutdown);
                        }
                    }
                }
            }
        }

        public bool IsLeader()
        {
            return _engine.CurrentState == RachisState.Leader;
        }

        public bool IsPassive()
        {
            return _engine.CurrentState == RachisState.Passive;
        }

        public async Task<(long Index, object Result)> SendToLeaderAsync(CommandBase cmd)
        {
            var response = await SendToLeaderAsyncInternal(cmd);

#if DEBUG

            if (Leader.GetConvertResult(cmd) == null && // if cmd specifies a convert, it explicitly handles this
                response.Result.ContainsBlittableObject())
            {
                throw new InvalidOperationException($"{nameof(ServerStore)}::{nameof(SendToLeaderAsync)}({response.Result}) should not return command results with blittable json objects. This is not supposed to happen and should be reported.");
            }

#endif

            return response;
        }

        //this is needed for cases where Result or any of its fields are blittable json.
        //(for example, this is needed for use with AddOrUpdateCompareExchangeCommand, since it returns BlittableJsonReaderObject as result)
        public Task<(long Index, object Result)> SendToLeaderAsync(TransactionOperationContext context, CommandBase cmd)
        {
            return SendToLeaderAsyncInternal(context, cmd);
        }

        public DynamicJsonArray GetClusterErrors()
        {
            return _engine.GetClusterErrorsFromLeader();
        }

        public async Task<(long ClusterEtag, string ClusterId, long newIdentityValue)> GenerateClusterIdentityAsync(string id, string databaseName, string raftRequestId)
        {
            var (etag, result) = await SendToLeaderAsync(new IncrementClusterIdentityCommand(databaseName, id.ToLower(), raftRequestId));

            if (result == null)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should generate a cluster-wide identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            return (etag, id.Substring(0, id.Length - 1) + '/' + result, (long)result);
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
            return new NodeInfo
            {
                NodeTag = NodeTag,
                TopologyId = GetClusterTopology().TopologyId,
                Certificate = Server.Certificate.CertificateForClients,
                NumberOfCores = ProcessorInfo.ProcessorCount,
                InstalledMemoryInGb = memoryInformation.InstalledMemory.GetDoubleValue(SizeUnit.Gigabytes),
                UsableMemoryInGb = memoryInformation.TotalPhysicalMemory.GetDoubleValue(SizeUnit.Gigabytes),
                BuildInfo = LicenseManager.BuildInfo,
                OsInfo = LicenseManager.OsInfo,
                ServerId = GetServerId(),
                CurrentState = CurrentRachisState,
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

        public async Task PutLicenseAsync(License license, string raftRequestId)
        {
            var command = new PutLicenseCommand(LicenseStorageKey, license, raftRequestId);

            var result = await SendToLeaderAsync(command);

            if (Logger.IsInfoEnabled)
                Logger.Info($"Updating license id: {license.Id}");

            await WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
        }

        public async Task PutNodeLicenseLimitsAsync(string nodeTag, DetailsPerNode detailsPerNode, int maxLicensedCores, string raftRequestId = null)
        {
            var nodeLicenseLimits = new NodeLicenseLimits
            {
                NodeTag = nodeTag,
                DetailsPerNode = detailsPerNode,
                LicensedCores = maxLicensedCores,
                AllNodes = GetClusterTopology().AllNodes.Keys.ToList()
            };

            var command = new UpdateLicenseLimitsCommand(LicenseLimitsStorageKey, nodeLicenseLimits, raftRequestId ?? RaftIdGenerator.NewId());

            var result = await SendToLeaderAsync(command);

            await WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
        }

        public DatabaseTopology LoadDatabaseTopology(string databaseName)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return Cluster.ReadDatabaseTopology(context, databaseName);
            }
        }

        private async Task<(long Index, object Result)> SendToLeaderAsyncInternal(CommandBase cmd)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                return await SendToLeaderAsyncInternal(context, cmd);
        }

        private async Task<(long Index, object Result)> SendToLeaderAsyncInternal(TransactionOperationContext context, CommandBase cmd)
        {
            //I think it is reasonable to expect timeout twice of error retry
            var timeoutTask = TimeoutManager.WaitFor(Engine.OperationTimeout, _shutdownNotification.Token);
            Exception requestException = null;
            while (true)
            {
                ServerShutdown.ThrowIfCancellationRequested();

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
                        if (logChange.IsCompleted == false)
                            ThrowTimeoutException(cmd, requestException);

                        continue;
                    }

                    var response = await SendToNodeAsync(context, cachedLeaderTag, cmd, reachedLeader);
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
                if (logChange.IsCompleted == false)
                {
                    ThrowTimeoutException(cmd, requestException);
                }
            }
        }

        private static void ThrowInvalidEngineState(CommandBase cmd)
        {
            throw new NotSupportedException("Cannot send command " + cmd.GetType().FullName + " to the cluster because this node is passive." + Environment.NewLine +
                                            "Passive nodes aren't members of a cluster and require admin action (such as creating a db) " +
                                            "to indicate that this node should create its own cluster");
        }

        private void ThrowTimeoutException(CommandBase cmd, Exception requestException)
        {
            throw new TimeoutException($"Could not send command {cmd.GetType().FullName} from {NodeTag} to leader because there is no leader, " +
                                       $"and we timed out waiting for one after {Engine.OperationTimeout}", requestException);
        }

        private async Task<(long Index, object Result)> SendToNodeAsync(TransactionOperationContext context, string engineLeaderTag, CommandBase cmd, Reference<bool> reachedLeader)
        {
            var djv = cmd.ToJson(context);
            var cmdJson = context.ReadObject(djv, "raft/command");

            ClusterTopology clusterTopology;
            using (context.OpenReadTransaction())
                clusterTopology = _engine.GetTopology(context);

            if (clusterTopology.Members.TryGetValue(engineLeaderTag, out string leaderUrl) == false)
                throw new InvalidOperationException("Leader " + engineLeaderTag + " was not found in the topology members");

            cmdJson.TryGet("Type", out string commandType);
            var command = new PutRaftCommand(cmdJson, _engine.Url, commandType);

            var serverCertificateChanged = Interlocked.Exchange(ref _serverCertificateChanged, 0) == 1;

            if (_clusterRequestExecutor == null
                || serverCertificateChanged
                || _clusterRequestExecutor.Url.Equals(leaderUrl, StringComparison.OrdinalIgnoreCase) == false)
            {
                _clusterRequestExecutor?.Dispose();
                _clusterRequestExecutor = CreateNewClusterRequestExecutor(leaderUrl);
            }

            try
            {
                await _clusterRequestExecutor.ExecuteAsync(command, context, token: ServerShutdown);
            }
            catch
            {
                reachedLeader.Value = command.HasReachLeader();
                throw;
            }

            return (command.Result.RaftCommandIndex, command.Result.Data);
        }

        private ClusterRequestExecutor CreateNewClusterRequestExecutor(string leaderUrl)
        {
            var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leaderUrl, Server.Certificate.Certificate);
            requestExecutor.DefaultTimeout = Engine.OperationTimeout;

            return requestExecutor;
        }

        private class PutRaftCommand : RavenCommand<PutRaftCommandResult>, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _command;
            private bool _reachedLeader;
            public override bool IsReadRequest => false;
            public bool HasReachLeader() => _reachedLeader;
            private readonly string _source;
            private readonly string _commandType;
            public PutRaftCommand(BlittableJsonReaderObject command, string source, string commandType)
            {
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
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteObject(_command);
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationCluster.PutRaftCommandResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public class PutRaftCommandResult
        {
            public long RaftCommandIndex { get; set; }

            public object Data { get; set; }
        }

        public Task WaitForTopology(Leader.TopologyModification state, CancellationToken token)
        {
            return _engine.WaitForTopology(state, token: token);
        }

        public Task WaitForState(RachisState rachisState, CancellationToken token)
        {
            return _engine.WaitForState(rachisState, token);
        }

        public void ClusterAcceptNewConnection(TcpConnectionOptions tcp, TcpConnectionHeaderMessage header, Action disconnect, EndPoint remoteEndpoint)
        {
            try
            {
                if (_engine == null)
                {
                    // on startup, the tcp listeners are initialized prior to the engine, so there could be a race.
                    disconnect();
                    return;
                }

                _engine.AcceptNewConnection(tcp.Stream, disconnect, remoteEndpoint);
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

        public async Task WaitForCommitIndexChange(RachisConsensus.CommitIndexModification modification, long value)
        {
            await _engine.WaitForCommitIndexChange(modification, value);
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
                    connectionInfo = ReplicationUtils.GetTcpInfoAsync(url, database, "Test-Connection", Server.Certificate.Certificate,
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
                    LoggingSource.Instance.GetLogger("testing-connection", "testing-connection"), database, result);
            }
            catch (Exception e)
            {
                result.Success = false;
                result.Error = $"Was able to connect to url '{url}', but exception was thrown while trying to connect to TCP port '{connectionInfo.Result.Url}':{Environment.NewLine}{e}";
            }

            return result;
        }

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
                [nameof(TcpConnectionInfo.NodeTag)] = NodeTag
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

        public DynamicJsonValue GetLogDetails(TransactionOperationContext context, int max = 100)
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

        public static IEnumerable<Client.ServerWide.Operations.MountPointUsage> GetMountPointUsageDetailsFor(StorageEnvironmentWithType environment, bool includeTempBuffers)
        {
            var fullPath = environment?.Environment.Options.BasePath.FullPath;
            if (fullPath == null)
                yield break;

            var driveInfo = environment.Environment.Options.DriveInfoByPath?.Value;
            var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(fullPath, driveInfo?.BasePath);
            if (diskSpaceResult == null)
                yield break;

            var sizeOnDisk = environment.Environment.GenerateSizeReport(includeTempBuffers);
            var usage = new Client.ServerWide.Operations.MountPointUsage
            {
                UsedSpace = sizeOnDisk.DataFileInBytes,
                DiskSpaceResult = new Client.ServerWide.Operations.DiskSpaceResult
                {
                    DriveName = diskSpaceResult.DriveName,
                    VolumeLabel = diskSpaceResult.VolumeLabel,
                    TotalFreeSpaceInBytes = diskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Bytes),
                    TotalSizeInBytes = diskSpaceResult.TotalSize.GetValue(SizeUnit.Bytes)
                },
                UsedSpaceByTempBuffers = 0
            };

            var journalPathUsage = DiskSpaceChecker.GetDiskSpaceInfo(environment.Environment.Options.JournalPath?.FullPath, driveInfo?.JournalPath);
            if (journalPathUsage != null)
            {
                if (diskSpaceResult.DriveName == journalPathUsage.DriveName)
                {
                    usage.UsedSpace += sizeOnDisk.JournalsInBytes;
                    usage.UsedSpaceByTempBuffers += includeTempBuffers ? sizeOnDisk.TempRecyclableJournalsInBytes : 0;
                }
                else
                {
                    yield return new Client.ServerWide.Operations.MountPointUsage
                    {
                        DiskSpaceResult = new Client.ServerWide.Operations.DiskSpaceResult
                        {
                            DriveName = journalPathUsage.DriveName,
                            VolumeLabel = journalPathUsage.VolumeLabel,
                            TotalFreeSpaceInBytes = journalPathUsage.TotalFreeSpace.GetValue(SizeUnit.Bytes),
                            TotalSizeInBytes = journalPathUsage.TotalSize.GetValue(SizeUnit.Bytes)
                        },
                        UsedSpaceByTempBuffers = includeTempBuffers ? sizeOnDisk.TempRecyclableJournalsInBytes : 0
                    };
                }
            }

            if (includeTempBuffers)
            {
                var tempBuffersDiskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(environment.Environment.Options.TempPath.FullPath, driveInfo?.TempPath);
                if (tempBuffersDiskSpaceResult != null)
                {
                    if (diskSpaceResult.DriveName == tempBuffersDiskSpaceResult.DriveName)
                    {
                        usage.UsedSpaceByTempBuffers += sizeOnDisk.TempBuffersInBytes;
                    }
                    else
                    {
                        yield return new Client.ServerWide.Operations.MountPointUsage
                        {
                            UsedSpaceByTempBuffers = sizeOnDisk.TempBuffersInBytes,
                            DiskSpaceResult = new Client.ServerWide.Operations.DiskSpaceResult
                            {
                                DriveName = tempBuffersDiskSpaceResult.DriveName,
                                VolumeLabel = tempBuffersDiskSpaceResult.VolumeLabel,
                                TotalFreeSpaceInBytes = tempBuffersDiskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Bytes),
                                TotalSizeInBytes = tempBuffersDiskSpaceResult.TotalSize.GetValue(SizeUnit.Bytes)
                            }
                        };
                    }
                }
            }

            yield return usage;
        }
    }
}
