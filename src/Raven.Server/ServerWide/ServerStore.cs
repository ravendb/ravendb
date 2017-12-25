using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Certes.Acme;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.Util;
using Raven.Client.Exceptions.Server;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Dashboard;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.Rachis;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private const string ResourceName = nameof(ServerStore);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStore>(ResourceName);

        public const string LicenseStorageKey = "License/Key";

        public const string LicenseLimitsStorageKey = "License/Limits/Key";

        private readonly CancellationTokenSource _shutdownNotification = new CancellationTokenSource();

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        private StorageEnvironment _env;

        private readonly NotificationsStorage _notificationsStorage;
        private readonly OperationsStorage _operationsStorage;

        private RequestExecutor _clusterRequestExecutor;

        public readonly RavenConfiguration Configuration;
        private readonly RavenServer _server;
        public readonly DatabasesLandlord DatabasesLandlord;
        public readonly NotificationCenter.NotificationCenter NotificationCenter;
        public readonly ServerDashboardNotifications ServerDashboardNotifications;
        public readonly LicenseManager LicenseManager;
        public readonly FeedbackSender FeedbackSender;
        public readonly SecretProtection Secrets;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        public long LastClientConfigurationIndex { get; private set; } = -2;

        public Operations Operations { get; }

        public ServerStore(RavenConfiguration configuration, RavenServer server)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            _server = server;

            DatabasesLandlord = new DatabasesLandlord(this);

            _notificationsStorage = new NotificationsStorage(ResourceName);

            NotificationCenter = new NotificationCenter.NotificationCenter(_notificationsStorage, null, ServerShutdown);

            ServerDashboardNotifications = new ServerDashboardNotifications(this, ServerShutdown);

            _operationsStorage = new OperationsStorage();

            Operations = new Operations(null, _operationsStorage, NotificationCenter, null);

            LicenseManager = new LicenseManager(this);

            FeedbackSender = new FeedbackSender();

            DatabaseInfoCache = new DatabaseInfoCache();

            Secrets = new SecretProtection(configuration.Security);

            _frequencyToCheckForIdleDatabases = Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;

        }

        public RavenServer Server => _server;

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public TransactionContextPool ContextPool;

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

        public Dictionary<string, ClusterNodeStatusReport> ClusterStats()
        {
            if (_engine.LeaderTag != NodeTag)
                throw new NotLeadingException($"Stats can be requested only from the raft leader {_engine.LeaderTag}");
            return ClusterMaintenanceSupervisor?.GetStats();
        }

        internal ClusterObserver Observer { get; set; }

        public async Task ClusterMaintenanceSetupTask()
        {
            while (true)
            {
                try
                {
                    if (_engine.LeaderTag != NodeTag)
                    {
                        await _engine.WaitForState(RachisState.Leader)
                            .WithCancellation(_shutdownNotification.Token);
                        continue;
                    }
                    using (ClusterMaintenanceSupervisor = new ClusterMaintenanceSupervisor(this, _engine.Tag, _engine.CurrentTerm))
                    using (Observer = new ClusterObserver(this, ClusterMaintenanceSupervisor, _engine, ContextPool, ServerShutdown))
                    {
                        var oldNodes = new Dictionary<string, string>();
                        while (_engine.LeaderTag == NodeTag)
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

                            if (newNodes.Count > 1)
                            {
                                // calculate only if we have more than one node
                                await LicenseManager.CalculateLicenseLimits(forceFetchingNodeInfo: true);
                            }

                            var leaderChanged = _engine.WaitForLeaveState(RachisState.Leader);

                            if (await Task.WhenAny(topologyChangedTask, leaderChanged)
                                    .WithCancellation(_shutdownNotification.Token) == leaderChanged)
                                break;
                        }
                    }
                }
                catch (TaskCanceledException)
                {
                    // ServerStore dispose?
                    throw;
                }
                catch (Exception)
                {
                    //
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

        public ClusterTopology GetClusterTopology(TransactionOperationContext context)
        {
            return _engine.GetTopology(context);
        }

        public async Task AddNodeToClusterAsync(string nodeUrl, string nodeTag = null, bool validateNotInTopology = true, bool asWatcher = false)
        {
            await _engine.AddToClusterAsync(nodeUrl, nodeTag, validateNotInTopology, asWatcher).WithCancellation(_shutdownNotification.Token);
        }

        public async Task RemoveFromClusterAsync(string nodeTag)
        {
            await _engine.RemoveFromClusterAsync(nodeTag).WithCancellation(_shutdownNotification.Token);
        }

        public void Initialize()
        {
            LowMemoryNotification.Initialize(ServerShutdown,
                Configuration.Memory.LowMemoryLimit.GetValue(SizeUnit.Bytes));

            if (Logger.IsInfoEnabled)
                Logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

            var path = Configuration.Core.DataDirectory.Combine("System");
            var storeAlertForLateRaise = new List<AlertRaised>();

            StorageEnvironmentOptions options;
            if (Configuration.Core.RunInMemory)
            {
                options = StorageEnvironmentOptions.CreateMemoryOnly();
            }
            else
            {
                options = StorageEnvironmentOptions.ForPath(path.FullPath);
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

                    var secret = new byte[buffer.Length - 32];
                    var entropy = new byte[32];
                    Array.Copy(buffer, 0, secret, 0, buffer.Length - 32);
                    Array.Copy(buffer, buffer.Length - 32, entropy, 0, 32);

                    try
                    {
                        options.MasterKey = Secrets.Unprotect(secret, entropy);
                    }
                    catch (Exception e)
                    {
                        throw new CryptographicException($"Unable to unprotect the secret key file {secretKey}. " +
                                                         "Was the server store encrypted using a different OS user? In that case, " +
                                                         "you must provide an unprotected key (rvn server put-key). " +
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

            try
            {
                var swapping = MemoryInformation.IsSwappingOnHddInsteadOfSsd();
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
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Server, null, null);
            options.ForceUsing32BitsPager = Configuration.Storage.ForceUsing32BitsPager;
            try
            {
                StorageEnvironment.MaxConcurrentFlushes = Configuration.Storage.MaxConcurrentFlushes;

                try
                {
                    _env = new StorageEnvironment(options);
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

            ContextPool = new TransactionContextPool(_env);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                // warm-up the json convertor, it takes about 250ms at first conversion.
                EntityToBlittable.ConvertEntityToBlittable(new DatabaseRecord(), DocumentConventions.Default, ctx);
            }

            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            _notificationsStorage.Initialize(_env, ContextPool);
            _operationsStorage.Initialize(_env, ContextPool);
            DatabaseInfoCache.Initialize(_env, ContextPool);

            NotificationCenter.Initialize();
            foreach (var alertRaised in storeAlertForLateRaise)
            {
                NotificationCenter.Add(alertRaised);
            }

            _engine = new RachisConsensus<ClusterStateMachine>(this);
            var myUrl = Configuration.Core.PublicServerUrl.HasValue ? Configuration.Core.PublicServerUrl.Value.UriValue : Configuration.Core.ServerUrls[0];
            _engine.Initialize(_env, Configuration, myUrl);

            LicenseManager.Initialize(_env, ContextPool);
            LatestVersionCheck.Check(this);
        }

        public void TriggerDatabases()
        {
            _engine.StateMachine.DatabaseChanged += DatabasesLandlord.ClusterOnDatabaseChanged;
            _engine.StateMachine.DatabaseChanged += OnDatabaseChanged;
            _engine.StateMachine.DatabaseValueChanged += DatabasesLandlord.ClusterOnDatabaseValueChanged;
            _engine.StateMachine.ValueChanged += OnValueChanged;

            _engine.TopologyChanged += OnTopologyChanged;
            _engine.StateChanged += OnStateChanged;

            if (IsLeader())
            {
                _engine.CurrentLeader.OnNodeStatusChange += OnTopologyChanged;
            }

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var db in _engine.StateMachine.GetDatabaseNames(context))
                {
                    DatabasesLandlord.ClusterOnDatabaseChanged(this, (db, 0, "Init"));
                }

                if (_engine.StateMachine.Read(context, Constants.Configuration.ClientId, out long clientConfigEtag) != null)
                    LastClientConfigurationIndex = clientConfigEtag;
            }

            Task.Run(ClusterMaintenanceSetupTask, ServerShutdown);
        }

        private void OnStateChanged(object sender, RachisConsensus.StateTransition state)
        {
            if (Engine.Log.IsInfoEnabled)
            {
                Engine.Log.Info($"State changed: {state.From} -> {state.To} in term {state.CurrentTerm}, because {state.Reason}");
            }

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                OnTopologyChanged(null, GetClusterTopology(context));

                // if we are in passive/candidate state, we prevent from tasks to be performed by this node.
                if (state.From == RachisState.Passive || state.To == RachisState.Passive ||
                    state.From == RachisState.Candidate || state.To == RachisState.Candidate)
                {
                    RefreshOutgoingTasks();
                }

                if (state.To == RachisState.LeaderElect)
                {
                    _engine.CurrentLeader.OnNodeStatusChange += OnTopologyChanged;
                }
            }
        }

        public Task RefreshOutgoingTasks()
        {
            return RefreshOutgoingTasksAsync();
        }

        public async Task RefreshOutgoingTasksAsync()
        {
            var tasks = new Dictionary<string, Task<DocumentDatabase>>();
            foreach (var db in DatabasesLandlord.DatabasesCache)
            {
                tasks.Add(db.Key, db.Value);
            }
            while (tasks.Any())
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

        private void OnTopologyChanged(object sender, ClusterTopology topologyJson)
        {
            NotificationCenter.Add(ClusterTopologyChanged.Create(topologyJson, LeaderTag,
                NodeTag, _engine.CurrentTerm, GetNodesStatuses(), LoadLicenseLimits()?.NodeLicenseDetails));
        }

        private void OnDatabaseChanged(object sender, (string DatabaseName, long Index, string Type) t)
        {
            switch (t.Type)
            {
                case nameof(DeleteDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.DatabaseName, DatabaseChangeType.Delete));
                    break;
                case nameof(AddDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.DatabaseName, DatabaseChangeType.Put));
                    break;
                case nameof(UpdateTopologyCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.DatabaseName, DatabaseChangeType.Update));
                    break;
                case nameof(RemoveNodeFromDatabaseCommand):
                    NotificationCenter.Add(DatabaseChanged.Create(t.DatabaseName, DatabaseChangeType.RemoveNode));
                    break;
            }
        }

        private void OnValueChanged(object sender, (long Index, string Type) t)
        {
            switch (t.Type)
            {
                case nameof(RecheckStatusOfServerCertificateCommand):
                case nameof(ConfirmReceiptServerCertificateCommand):
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var cert = Cluster.GetItem(context, "server/cert");
                        if (cert == null)
                            return; // was already processed?
                        if (cert.TryGet("Confirmations", out int confirmations) == false)
                            throw new InvalidOperationException("Expected to get confirmations count");

                        if (GetClusterTopology(context).AllNodes.Count > confirmations)
                        {
                            if (Server.Certificate?.Certificate != null &&
                                (Server.Certificate.Certificate.NotAfter - DateTime.Now).TotalDays > 3)
                                return; // we still have time for all the nodes to update themselves 

                        }

                        if (cert.TryGet("Certificate", out string certBase64) == false ||
                            cert.TryGet("Thumbprint", out string certThumbprint) == false)
                            throw new InvalidOperationException("Invalid server cert value, expected to get Certificate and Thumbprint properties");

                        if (certThumbprint == Server.Certificate?.Certificate?.Thumbprint)
                            return;// already replaced it, nothing to do

                        // and now we have to replace the cert...
                        if (string.IsNullOrEmpty(Configuration.Security.CertificatePath))
                        {
                            NotificationCenter.Add(AlertRaised.Create(
                                null,
                                "Unable to refresh server certificate",
                                "Cluster wanted to install updated server certificate, but no path has been configured",
                                AlertType.ClusterTopologyWarning,
                                NotificationSeverity.Error,
                                "Cluster.Certificate.Install.Error"));
                            return;
                        }


                        var bytesToSave = Convert.FromBase64String(certBase64);
                        var newClusterCertificate = new X509Certificate2(bytesToSave, (string)null, X509KeyStorageFlags.Exportable);

                        if (Logger.IsOperationsEnabled)
                            Logger.Operations($"Replacing the certificate used by the server to: {newClusterCertificate.FriendlyName} - {newClusterCertificate.Thumbprint}");


                        if (string.IsNullOrEmpty(Configuration.Security.CertificatePassword) == false)
                        {
                            bytesToSave = newClusterCertificate.Export(X509ContentType.Pkcs12, Configuration.Security.CertificatePassword);
                        }

                        using (var certStream = File.Create(Path.Combine(AppContext.BaseDirectory, Configuration.Security.CertificatePath)))
                        {
                            certStream.Write(bytesToSave, 0, bytesToSave.Length);
                            certStream.Flush(true);
                        }

                        Server.SetCertificate(newClusterCertificate, bytesToSave, Configuration.Security.CertificatePassword);

                    }
                    break;
                case nameof(InstallUpdatedServerCertificateCommand):
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var cert = Cluster.GetItem(context, "server/cert");
                        if (cert == null)
                            return; // was already processed?
                        if (cert.TryGet("Thumbprint", out string certThumbprint) == false)
                            throw new InvalidOperationException("Invalid server cert value, expected to get Thumbprint property");

                        if (cert.TryGet("Certificate", out string base64Cert) == false)
                            throw new InvalidOperationException("Invalid server cert value, expected to get Certificate property");

                        var certificate = new X509Certificate2(Convert.FromBase64String(base64Cert));

                        var now = DateTime.UtcNow;
                        if (certificate.NotBefore.ToUniversalTime() > now)
                        {
                            if (Logger.IsOperationsEnabled)
                            {
                                Logger.Operations($"Unable to confirm certificate update because the NotBefore property is set " +
                                                  $"to {certificate.NotBefore.ToUniversalTime():O} and now it is {now:O}. Will try again later");
                            }
                            return;
                        }

                        // we got it, now let us let the leader know about it
                        SendToLeaderAsync(new ConfirmReceiptServerCertificateCommand(certThumbprint));
                    }
                    break;
                case nameof(PutClientConfigurationCommand):
                    LastClientConfigurationIndex = t.Index;
                    break;
                case nameof(PutLicenseCommand):
                    LicenseManager.ReloadLicense();
                    break;
                case nameof(PutLicenseLimitsCommand):
                    LicenseManager.ReloadLicenseLimits();
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        OnTopologyChanged(null, GetClusterTopology(context));
                    }
                    break;
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
                    Sodium.ZeroMemory((byte*)pBase64, base64.Length * sizeof(char));
                    Sodium.ZeroMemory(pKey, key.Length);
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
            if (secretKey.Length != 256 / 8)
                throw new ArgumentException($"Key size must be 256 bits, but was {secretKey.Length * 8}", nameof(secretKey));

            byte[] key;
            if (cloneKey)
                Sodium.CloneKey(out key, secretKey);
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
                    bool areEqual = Sparrow.Memory.Compare(pKey, pExistingKey, key.Length) == 0;
                    Sodium.ZeroMemory(pExistingKey, key.Length);
                    if (areEqual)
                    {
                        Sodium.ZeroMemory(pKey, key.Length);
                        return;
                    }
                }
            }

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");
            var record = Cluster.ReadDatabase(context, name);

            if (overwrite == false && tree.Read(name) != null)
                throw new InvalidOperationException($"Attempt to overwrite secret key {name}, which isn\'t permitted (you\'ll lose access to the encrypted db).");

            if (record != null && record.Encrypted == false)
                throw new InvalidOperationException($"Cannot modify key {name} where there is an existing database that is not encrypted");

            var hashLen = Sodium.crypto_generichash_bytes_max();
            var hash = new byte[hashLen + key.Length];
            fixed (byte* pHash = hash)
            fixed (byte* pKey = key)
            {
                try
                {
                    if (Sodium.crypto_generichash(pHash, (UIntPtr)hashLen, pKey, (ulong)key.Length, null, UIntPtr.Zero) != 0)
                        throw new InvalidOperationException("Failed to hash key");

                    Sparrow.Memory.Copy(pHash + hashLen, pKey, key.Length);

                    var entropy = Sodium.GenerateRandomBuffer(256);

                    var protectedData = Secrets.Protect(hash, entropy);

                    var ms = new MemoryStream();
                    ms.Write(entropy, 0, entropy.Length);
                    ms.Write(protectedData, 0, protectedData.Length);
                    ms.Position = 0;

                    tree.Add(name, ms);
                }
                finally
                {
                    Sodium.ZeroMemory(pHash, hash.Length);
                    Sodium.ZeroMemory(pKey, key.Length);
                }
            }
        }


        public unsafe byte[] GetSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            var tree = context.Transaction.InnerTransaction.ReadTree("SecretKeys");

            var readResult = tree?.Read(name);
            if (readResult == null)
                return null;

            const int numberOfBits = 256;
            var entropy = new byte[numberOfBits / 8];
            var reader = readResult.Reader;
            reader.Read(entropy, 0, entropy.Length);
            var protectedData = new byte[reader.Length - entropy.Length];
            reader.Read(protectedData, 0, protectedData.Length);

            var data = Secrets.Unprotect(protectedData, entropy);

            var hashLen = Sodium.crypto_generichash_bytes_max();

            fixed (byte* pData = data)
            fixed (byte* pHash = new byte[hashLen])
            {
                try
                {
                    if (Sodium.crypto_generichash(pHash, (UIntPtr)hashLen, pData + hashLen, (ulong)(data.Length - hashLen), null, UIntPtr.Zero) != 0)
                        throw new InvalidOperationException($"Unable to compute hash for {name}");

                    if (Sodium.sodium_memcmp(pData, pHash, (UIntPtr)hashLen) != 0)
                        throw new InvalidOperationException($"Unable to validate hash after decryption for {name}, user store changed?");

                    var buffer = new byte[data.Length - hashLen];
                    fixed (byte* pBuffer = buffer)
                    {
                        Sparrow.Memory.Copy(pBuffer, pData + hashLen, buffer.Length);
                    }
                    return buffer;
                }
                finally
                {
                    Sodium.ZeroMemory(pData, data.Length);
                }
            }
        }

        public void DeleteSecretKey(TransactionOperationContext context, string name)
        {
            Debug.Assert(context.Transaction != null);

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");

            tree.Delete(name);
        }

        public Task<(long Index, object Result)> DeleteDatabaseAsync(string db, bool hardDelete, string[] fromNodes)
        {
            var deleteCommand = new DeleteDatabaseCommand(db)
            {
                HardDelete = hardDelete,
                FromNodes = fromNodes
            };
            return SendToLeaderAsync(deleteCommand);
        }

        public Task<(long Index, object Result)> UpdateExternalReplication(string dbName, BlittableJsonReaderObject blittableJson, out ExternalReplication watcher)
        {
            if (blittableJson.TryGet(nameof(UpdateExternalReplicationCommand.Watcher), out BlittableJsonReaderObject watcherBlittable) == false)
            {
                throw new InvalidDataException($"{nameof(UpdateExternalReplicationCommand.Watcher)} was not found.");
            }

            watcher = JsonDeserializationClient.ExternalReplication(watcherBlittable);
            var addWatcherCommand = new UpdateExternalReplicationCommand(dbName)
            {
                Watcher = watcher
            };
            return SendToLeaderAsync(addWatcherCommand);
        }

        public Task<(long Index, object Result)> DeleteOngoingTask(long taskId, string taskName, OngoingTaskType taskType, string dbName)
        {
            var deleteTaskCommand =
                taskType == OngoingTaskType.Subscription ?
                    (CommandBase)new DeleteSubscriptionCommand(dbName, taskName) :
                    new DeleteOngoingTaskCommand(taskId, taskType, dbName);

            return SendToLeaderAsync(deleteTaskCommand);
        }

        public Task<(long Index, object Result)> ToggleTaskState(long taskId, string taskName, OngoingTaskType type, bool disable, string dbName)
        {
            var disableEnableCommand =
                type == OngoingTaskType.Subscription ?
                    (CommandBase)new ToggleSubscriptionStateCommand(taskName, disable, dbName) :
                    new ToggleTaskStateCommand(taskId, type, disable, dbName);

            return SendToLeaderAsync(disableEnableCommand);
        }

        public Task<(long Index, object Result)> PromoteDatabaseNode(string dbName, string nodeTag)
        {
            var promoteDatabaseNodeCommand = new PromoteDatabaseNodeCommand(dbName)
            {
                NodeTag = nodeTag
            };
            return SendToLeaderAsync(promoteDatabaseNodeCommand);
        }

        public Task<(long Index, object Result)> ModifyConflictSolverAsync(string dbName, ConflictSolver solver)
        {
            var conflictResolverCommand = new ModifyConflictSolverCommand(dbName)
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

        public Task<(long Index, object Result)> ModifyDatabaseExpiration(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            var editExpiration = new EditExpirationCommand(JsonDeserializationCluster.ExpirationConfiguration(configurationJson), name);
            return SendToLeaderAsync(editExpiration);
        }

        public async Task<(long, object)> ModifyPeriodicBackup(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            var modifyPeriodicBackup = new UpdatePeriodicBackupCommand(JsonDeserializationCluster.PeriodicBackupConfiguration(configurationJson), name);
            return await SendToLeaderAsync(modifyPeriodicBackup);
        }

        public async Task<(long, object)> AddEtl(TransactionOperationContext context,
            string databaseName, BlittableJsonReaderObject etlConfiguration)
        {
            UpdateDatabaseCommand command;

            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    command = new AddRavenEtlCommand(JsonDeserializationCluster.RavenEtlConfiguration(etlConfiguration), databaseName);
                    break;
                case EtlType.Sql:
                    command = new AddSqlEtlCommand(JsonDeserializationCluster.SqlEtlConfiguration(etlConfiguration), databaseName);
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }

            return await SendToLeaderAsync(command);
        }

        public async Task<(long, object)> UpdateEtl(TransactionOperationContext context, string databaseName, long id, BlittableJsonReaderObject etlConfiguration)
        {
            UpdateDatabaseCommand command;

            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    command = new UpdateRavenEtlCommand(id, JsonDeserializationCluster.RavenEtlConfiguration(etlConfiguration), databaseName);
                    break;
                case EtlType.Sql:
                    command = new UpdateSqlEtlCommand(id, JsonDeserializationCluster.SqlEtlConfiguration(etlConfiguration), databaseName);
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }

            return await SendToLeaderAsync(command);
        }

        public Task<(long, object)> RemoveEtlProcessState(TransactionOperationContext context, string databaseName, string configurationName, string transformationName)
        {
            var command = new RemoveEtlProcessStateCommand(databaseName, configurationName, transformationName);

            return SendToLeaderAsync(command);
        }

        public Task<(long, object)> ModifyDatabaseRevisions(JsonOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            var editRevisions = new EditRevisionsConfigurationCommand(JsonDeserializationCluster.RevisionsConfiguration(configurationJson), name);
            return SendToLeaderAsync(editRevisions);
        }

        public async Task<(long, object)> PutConnectionString(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject connectionString)
        {
            if (connectionString.TryGet(nameof(ConnectionString.Type), out string type) == false)
                throw new InvalidOperationException($"Connection string must have {nameof(ConnectionString.Type)} field");

            if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

            UpdateDatabaseCommand command;

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    command = new PutRavenConnectionString(JsonDeserializationCluster.RavenConnectionString(connectionString), databaseName);
                    break;
                case ConnectionStringType.Sql:
                    var connection = JsonDeserializationCluster.SqlConnectionString(connectionString);
                    try
                    {
                        using (new SqlConnection(connection.ConnectionString))
                        {
                            // if connection string is invalid then the above 'new' will throw..
                        }
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Invalid connection string. " + e.Message);
                    }

                    command = new PutSqlConnectionString(connection, databaseName);

                    break;
                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
            }

            return await SendToLeaderAsync(command);
        }

        public async Task<(long, object)> RemoveConnectionString(string databaseName, string connectionStringName, string type)
        {
            if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

            UpdateDatabaseCommand command;

            var databaseRecord = LoadDatabaseRecord(databaseName, out var _);

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:

                    // Don't delete the connection string if used by tasks types: External Replication || Raven Etl
                    foreach (var ravenETlTask in databaseRecord.RavenEtls)
                    {
                        if (ravenETlTask.ConnectionStringName == connectionStringName)
                        {
                            throw new InvalidOperationException($"Can't delete connection string: {connectionStringName}. It is used by task: {ravenETlTask.Name}");
                        }
                    }

                    foreach (var replicationTask in databaseRecord.ExternalReplications)
                    {
                        if (replicationTask.ConnectionStringName == connectionStringName)
                        {
                            throw new InvalidOperationException($"Can't delete connection string: {connectionStringName}. It is used by task: {replicationTask.Name}");
                        }
                    }

                    command = new RemoveRavenConnectionString(connectionStringName, databaseName);
                    break;

                case ConnectionStringType.Sql:

                    // Don't delete the connection string if used by tasks types: SQL Etl
                    foreach (var sqlETlTask in databaseRecord.SqlEtls)
                    {
                        if (sqlETlTask.ConnectionStringName == connectionStringName)
                        {
                            throw new InvalidOperationException($"Can't delete connection string: {connectionStringName}. It is used by task: {sqlETlTask.Name}");
                        }
                    }

                    command = new RemoveSqlConnectionString(connectionStringName, databaseName);
                    break;

                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
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
                    var toDispose = new List<IDisposable>
                    {
                        _engine,
                        NotificationCenter,
                        LicenseManager,
                        DatabasesLandlord,
                        _env,
                        ContextPool,
                        ByteStringMemoryCache.Cleaner
                    };

                    var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {nameof(ServerStore)}.");

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

                    exceptionAggregator.Execute(() => _shutdownNotification.Dispose());

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

                        if (DatabaseNeedsToRunIdleOperations(database))
                            database.RunIdleOperations();
                    }

                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error during idle operation run for " + db.Key, e);
                    }
                }

                try
                {
                    var maxTimeDatabaseCanBeIdle = Configuration.Databases.MaxIdleTime.AsTimeSpan;

                    var databasesToCleanup = DatabasesLandlord.LastRecentlyUsed
                        .Where(x => SystemTime.UtcNow - x.Value > maxTimeDatabaseCanBeIdle)
                        .Select(x => x.Key)
                        .ToArray();

                    foreach (var db in databasesToCleanup)
                    {
                        if (DatabasesLandlord.DatabasesCache.TryGetValue(db, out Task<DocumentDatabase> resourceTask) &&
                            resourceTask != null &&
                            resourceTask.Status == TaskStatus.RanToCompletion &&
                            resourceTask.Result.PeriodicBackupRunner != null &&
                            resourceTask.Result.PeriodicBackupRunner.HasRunningBackups())
                        {
                            // there are running backups for this database
                            continue;
                        }

                        // intentionally inside the loop, so we get better concurrency overall
                        // since shutting down a database can take a while
                        if (resourceTask == null ||
                            resourceTask.Result.Configuration.Core.RunInMemory)
                            continue;
                        var idleDbInstance = resourceTask.Result;
                        if (SystemTime.UtcNow - DatabasesLandlord.LastWork(idleDbInstance) < maxTimeDatabaseCanBeIdle)
                            continue;

                        DatabasesLandlord.UnloadDirectly(db);
                    }

                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error during idle operations for the server", e);
                }
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

        private static bool DatabaseNeedsToRunIdleOperations(DocumentDatabase database)
        {
            var now = DateTime.UtcNow;

            var envs = database.GetAllStoragesEnvironment();

            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            return ((now - maxLastWork).TotalMinutes > 5) || ((now - database.LastIdleTime).TotalMinutes > 10);
        }

        public void SecedeFromCluster()
        {
            Engine.Bootstrap(GetNodeHttpServerUrl(), forNewCluster: true);
        }

        public Task<(long Index, object Result)> WriteDatabaseRecordAsync(
            string databaseName, DatabaseRecord record, long? index,
            Dictionary<string, ExpandoObject> databaseValues = null, bool isRestore = false)
        {
            if (databaseValues == null)
                databaseValues = new Dictionary<string, ExpandoObject>();

            Debug.Assert(record.Topology != null);
            record.Topology.Stamp = new LeaderStamp
            {
                Term = _engine.CurrentTerm,
                LeadersTicks = _engine.CurrentLeader?.LeaderShipDuration ?? 0
            };

            var addDatabaseCommand = new AddDatabaseCommand
            {
                Name = databaseName,
                RaftCommandIndex = index,
                Record = record,
                DatabaseValues = databaseValues,
                IsRestore = isRestore
            };

            return SendToLeaderAsync(addDatabaseCommand);
        }

        public void EnsureNotPassive(string publicServerUrl = null)
        {
            if (_engine.CurrentState != RachisState.Passive)
                return;

            _engine.Bootstrap(publicServerUrl ?? _server.ServerStore.GetNodeHttpServerUrl());
            LicenseManager.TryActivateLicense();

            // we put a certificate in the local state to tell the server who to trust, and this is done before
            // the cluster exists (otherwise the server won't be able to receive initial requests). Only when we 
            // create the cluster, we register those local certificates in the cluster.
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                using (ctx.OpenReadTransaction())
                {
                    foreach (var localCertKey in Cluster.GetCertificateKeysFromLocalState(ctx))
                    {
                        // if there are trusted certificates in the local state, we will register them in the cluster now
                        using (var localCertificate = Cluster.GetLocalState(ctx, localCertKey))
                        {
                            var certificateDefinition = JsonDeserializationServer.CertificateDefinition(localCertificate);
                            PutValueInClusterAsync(new PutCertificateCommand(localCertKey, certificateDefinition)).Wait(ServerShutdown);
                        }
                    }
                }
            }

            EnsureServerCertificateIsInClusterState($"Server Certificate for Node {_engine.Tag}");
        }

        public void EnsureServerCertificateIsInClusterState(string name)
        {
            if (Server.Certificate?.Certificate == null)
                return;

            // Also need to register my own certificate in the cluster, for other nodes to trust me
            RegisterServerCertificateInCluster(Server.Certificate.Certificate, name).Wait(ServerShutdown);
        }

        public Task RegisterServerCertificateInCluster(X509Certificate2 certificateCertificate, string name)
        {
            var myCertificate = new CertificateDefinition
            {
                Certificate = Convert.ToBase64String(certificateCertificate.Export(X509ContentType.Cert)),
                Thumbprint = certificateCertificate.Thumbprint,
                NotAfter = certificateCertificate.NotAfter,
                Name = name,
                SecurityClearance = SecurityClearance.ClusterNode
            };

            return PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + myCertificate.Thumbprint, myCertificate));
        }

        public bool IsLeader()
        {
            return _engine.CurrentState == RachisState.Leader;
        }

        public bool IsPassive()
        {
            return _engine.CurrentState == RachisState.Passive;
        }

        public Task<(long Index, object Result)> SendToLeaderAsync(CommandBase cmd)
        {
            return SendToLeaderAsyncInternal(cmd);
        }

        public DynamicJsonArray GetClusterErrors()
        {
            return _engine.GetClusterErrorsFromLeader();
        }

        public async Task<(long ClusterEtag, string ClusterId, long newIdentityValue)> GenerateClusterIdentityAsync(string id, string databaseName)
        {
            var (etag, result) = await SendToLeaderAsync(new IncrementClusterIdentityCommand(databaseName, id.ToLower()));

            if (result == null)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should generate a cluster-wide identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            return (etag, id.Substring(0, id.Length - 1) + '/' + result, (long)result);
        }

        public async Task<long> UpdateClusterIdentityAsync(string id, string databaseName, long newIdentity)
        {
            var identities = new Dictionary<string, long>
            {
                [id] = newIdentity
            };

            var (_, result) = await SendToLeaderAsync(new UpdateClusterIdentityCommand(databaseName, identities));

            if (result == null)
            {
                throw new InvalidOperationException(
                    $"Expected to get result from raft command that should update a cluster-wide identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");
            }

            var newIdentitiesResult = result as Dictionary<string, long> ?? throw new InvalidOperationException(
                                 $"Expected to get result from raft command that should update a cluster-wide identity, but got invalid result structure for {id}. Leader is {LeaderTag}, Current node tag is {NodeTag}.");

            if (newIdentitiesResult.TryGetValue(UpdateValueForDatabaseCommand.GetStorageKey(databaseName, id), out long newIdentityValue) == false)
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

        public async Task<List<long>> GenerateClusterIdentitiesBatchAsync(string databaseName, List<string> ids)
        {
            var (_, identityInfoResult) = await SendToLeaderAsync(new IncrementClusterIdentitiesBatchCommand(databaseName, ids));

            var identityInfo = identityInfoResult as List<long> ?? throw new InvalidOperationException(
                    $"Expected to get result from raft command that should generate a cluster-wide batch identity, but didn't. Leader is {LeaderTag}, Current node tag is {NodeTag}.");

            return identityInfo;
        }

        public License LoadLicense()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var licenseBlittable = Cluster.Read(context, LicenseStorageKey);
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

        public async Task PutLicenseAsync(License license)
        {
            var command = new PutLicenseCommand(LicenseStorageKey, license);

            var result = await SendToLeaderAsync(command);

            if (Logger.IsInfoEnabled)
                Logger.Info($"Updating license id: {license.Id}");

            await WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
        }

        public void PutLicenseLimits(LicenseLimits licenseLimits)
        {
            if (IsLeader() == false)
                throw new InvalidOperationException("Only the leader can set the license limits!");

            var command = new PutLicenseLimitsCommand(LicenseLimitsStorageKey, licenseLimits);
            _engine.Put(command);
        }

        public async Task PutLicenseLimitsAsync(LicenseLimits licenseLimits)
        {
            if (IsLeader() == false)
                throw new InvalidOperationException("Only the leader can set the license limits!");

            var command = new PutLicenseLimitsCommand(LicenseLimitsStorageKey, licenseLimits);

            var result = await SendToLeaderAsync(command);

            await WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
        }

        public DatabaseRecord LoadDatabaseRecord(string databaseName, out long etag)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return Cluster.ReadDatabase(context, databaseName, out etag);
            }
        }

        private async Task<(long Index, object Result)> SendToLeaderAsyncInternal(CommandBase cmd)
        {
            //I think it is reasonable to expect timeout twice of error retry
            var timeoutTask = TimeoutManager.WaitFor(Engine.OperationTimeout, _shutdownNotification.Token);

            Exception requestException = null;
            while (true)
            {
                ServerShutdown.ThrowIfCancellationRequested();

                if (_engine.CurrentState == RachisState.Leader)
                {
                    return await _engine.PutAsync(cmd);
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

                    var result = await SendToNodeAsync(cachedLeaderTag, cmd, reachedLeader);
                    return (result.Index, cmd.FromRemote(result.Result));
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Tried to send message to leader, retrying", ex);

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

        private async Task<(long Index, object Result)> SendToNodeAsync(string engineLeaderTag, CommandBase cmd, Reference<bool> reachedLeader)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var djv = cmd.ToJson(context);
                var cmdJson = context.ReadObject(djv, "raft/command");

                ClusterTopology clusterTopology;
                using (context.OpenReadTransaction())
                    clusterTopology = _engine.GetTopology(context);

                if (clusterTopology.Members.TryGetValue(engineLeaderTag, out string leaderUrl) == false)
                    throw new InvalidOperationException("Leader " + engineLeaderTag + " was not found in the topology members");

                var command = new PutRaftCommand(cmdJson);

                if (_clusterRequestExecutor == null
                    || _clusterRequestExecutor.Url.Equals(leaderUrl, StringComparison.OrdinalIgnoreCase) == false)
                {
                    _clusterRequestExecutor?.Dispose();
                    _clusterRequestExecutor = ClusterRequestExecutor.CreateForSingleNode(leaderUrl, Server.Certificate.Certificate);
                    _clusterRequestExecutor.DefaultTimeout = Engine.OperationTimeout;
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
        }

        private class PutRaftCommand : RavenCommand<PutRaftCommandResult>
        {
            private readonly BlittableJsonReaderObject _command;
            private bool _reachedLeader;
            public override bool IsReadRequest => false;
            public bool HasReachLeader() => _reachedLeader;
            public PutRaftCommand(BlittableJsonReaderObject command)
            {
                _command = command;
            }

            public override void OnResponseFailure(HttpResponseMessage response)
            {
                if (response.Headers.Contains("Reached-Leader") == false)
                    return;
                _reachedLeader = response.Headers.GetValues("Reached-Leader").Contains("true");
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/rachis/send";

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
        }

        public class PutRaftCommandResult
        {
            public long RaftCommandIndex { get; set; }

            public object Data { get; set; }
        }

        public Task WaitForTopology(Leader.TopologyModification state)
        {
            return _engine.WaitForTopology(state);
        }

        public Task WaitForState(RachisState rachisState)
        {
            return _engine.WaitForState(rachisState);
        }

        public void ClusterAcceptNewConnection(Stream client)
        {
            _engine.AcceptNewConnection(client);
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

        private static void ThrowInvalidTcpUrlOnStartup()
        {
            throw new InvalidOperationException("The server has yet to complete startup, cannot get NodeTcpServerUtl");
        }

        public DynamicJsonValue GetTcpInfoAndCertificates(string clientRequestedNodeUrl)
        {
            var tcpServerUrl = GetNodeTcpServerUrl(clientRequestedNodeUrl);
            if (tcpServerUrl.StartsWith("tcp://localhost.fiddler:", StringComparison.OrdinalIgnoreCase))
                tcpServerUrl = tcpServerUrl.Remove(15, 8);

            return new DynamicJsonValue
            {
                [nameof(TcpConnectionInfo.Url)] = tcpServerUrl,
                [nameof(TcpConnectionInfo.Certificate)] = _server.Certificate.CertificateForClients
            };
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

        public void EnsureRegistrationOfCertificatesForTrustedIssuers(X509Certificate2 certificate)
        {
            if (certificate == null)
                return;
            try
            {
                //This is ugly, but we have to deal with the TRUSTED_ISSUERS and need to register the server and client certificates 
                //properly so the SSL on Windows 7 and Linux will handle that until we upgrade

                CertificateUtils.RegisterCertificateInOperatingSystem(new X509Certificate2(certificate.Export(X509ContentType.Cert)));

                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var item in Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue))
                    {
                        var def = JsonDeserializationServer.CertificateDefinition(item.Value);

                        var cert = new X509Certificate2(Convert.FromBase64String(def.Certificate));
                        CertificateUtils.RegisterCertificateInOperatingSystem(cert);
                    }

                    foreach (var localCertKey in Cluster.GetCertificateKeysFromLocalState(context))
                    {
                        // if there are trusted certificates in the local state, we will register them in the cluster now
                        using (var localCertificate = Cluster.GetLocalState(context, localCertKey))
                        {
                            var def = JsonDeserializationServer.CertificateDefinition(localCertificate);
                            var cert = new X509Certificate2(Convert.FromBase64String(def.Certificate));
                            CertificateUtils.RegisterCertificateInOperatingSystem(cert);
                        }
                    }
                }
            }
            catch (Exception e)
            {
               if(Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to registered certificates in operating system to deal with TRUSTED_ISSUERS", e);
            }

        }
    }
}
