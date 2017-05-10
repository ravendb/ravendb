using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Client.Util;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter;
using Raven.Server.Rachis;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintance;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private const string ResourceName = nameof(ServerStore);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStore>(ResourceName);

        private readonly CancellationTokenSource _shutdownNotification = new CancellationTokenSource();

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        private StorageEnvironment _env;

        private readonly NotificationsStorage _notificationsStorage;

        private RequestExecutor _clusterRequestExecutor;

        public readonly RavenConfiguration Configuration;
        private readonly RavenServer _ravenServer;
        public readonly DatabasesLandlord DatabasesLandlord;
        public readonly NotificationCenter.NotificationCenter NotificationCenter;
        public readonly LicenseManager LicenseManager;
        public readonly FeedbackSender FeedbackSender;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        public ServerStore(RavenConfiguration configuration, RavenServer ravenServer)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            Configuration = configuration;
            _ravenServer = ravenServer;

            DatabasesLandlord = new DatabasesLandlord(this);

            _notificationsStorage = new NotificationsStorage(ResourceName);

            NotificationCenter = new NotificationCenter.NotificationCenter(_notificationsStorage, ResourceName, ServerShutdown);

            LicenseManager = new LicenseManager(NotificationCenter);

            FeedbackSender = new FeedbackSender();

            DatabaseInfoCache = new DatabaseInfoCache();

            _frequencyToCheckForIdleDatabases = Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;
            
        }

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public TransactionContextPool ContextPool;

        public long LastRaftCommitEtag
        {
            get
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    return _engine.GetLastCommitIndex(context);
            }
        }

        public ClusterStateMachine Cluster => _engine.StateMachine;
        public string LeaderTag => _engine.LeaderTag;

        public string NodeTag => _engine.Tag;

        public bool Disposed => _disposed;

        private Timer _timer;
        private RachisConsensus<ClusterStateMachine> _engine;
        private bool _disposed;      
        public RachisConsensus<ClusterStateMachine> Engine => _engine;

        private ClusterMaintenanceMaster _clusterMaintenanceMaster;
        public Dictionary<string, ClusterNodeStatusReport> ClusterStats()
        {
            if (_engine.LeaderTag != NodeTag)
                throw new NotLeadingException($"Stats can be requested only from the raft leader {_engine.LeaderTag}");
            return _clusterMaintenanceMaster?.GetStats();
        }
        public async Task ClusterMaintanceSetupTask()
        {
            while (true)
            {
                try
                {
                    if (_engine.LeaderTag != NodeTag)
                    {
                        await _engine.WaitForState(RachisConsensus.State.Leader);
                        continue;
                    }
                    using (_clusterMaintenanceMaster = new ClusterMaintenanceMaster(this,_engine.Tag, _engine.CurrentTerm))
                    using (new ClusterObserver(_clusterMaintenanceMaster,this,_engine,ContextPool,ServerShutdown))
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
                            foreach (var node in nodesChanges.removedValues)
                            {
                                _clusterMaintenanceMaster.RemoveFromCluster(node.Key);
                            }
                            foreach (var node in nodesChanges.addedValues)
                            {
                                var task =
                                    _clusterMaintenanceMaster
                                        .AddToCluster(node.Key, clusterTopology.GetUrlFromTag(node.Key))
                                        .ContinueWith(t =>
                                        {
                                            if(Logger.IsInfoEnabled)
                                                Logger.Info($"ClusterMaintenanceSetupTask() => Failed to add to cluster node key = {node.Key}",t.Exception);
                                        },TaskContinuationOptions.OnlyOnFaulted);
                                GC.KeepAlive(task);
                            }

                            var leaderChanged = _engine.WaitForLeaveState(RachisConsensus.State.Leader);
                            if (await Task.WhenAny(topologyChangedTask, leaderChanged) == leaderChanged)
                                break;
                        }
                    }
                }
                catch (TaskCanceledException)
                {// ServerStore dispose?
                    throw;
                }
                catch (Exception)
                {
                    //
                }
            }   
        }

        public ClusterTopology GetClusterTopology(TransactionOperationContext context)
        {
            return _engine.GetTopology(context);
        }

        public async Task AddNodeToClusterAsync(string nodeUrl)
        {
            await _engine.AddToClusterAsync(nodeUrl);
        }

        public async Task RemoveFromClusterAsync(string nodeTag)
        {
            await _engine.RemoveFromClusterAsync(nodeTag);
        }

        public void Initialize()
        {
            LowMemoryNotification.Initialize(ServerShutdown, 
                Configuration.Memory.LowMemoryDetection.GetValue(SizeUnit.Bytes),
                Configuration.Memory.PhysicalRatioForLowMemDetection);

            if (Logger.IsInfoEnabled)
                Logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

            var path = Configuration.Core.DataDirectory.Combine("System");


            List<AlertRaised> storeAlertForLateRaise = new List<AlertRaised>();

            var options = Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(path.FullPath);

            options.OnNonDurableFileSystemError += (obj, e) =>
            {
                var alert = AlertRaised.Create("Non Durable File System - System Database",
                    e.Message,
                    AlertType.NonDurableFileSystem,
                    NotificationSeverity.Warning,
                    "NonDurable Error System",
                    details: new MessageDetails{Message=e.Details});
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
                var alert = AlertRaised.Create("Database Recovery Error - System Database",
                    e.Message,
                    AlertType.NonDurableFileSystem,
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

            options.SchemaVersion = 2;
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
                    throw new DatabaseLoadFailureException("Failed to load system database " + Environment.NewLine + $"At {options.BasePath}", e);
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


            _engine = new RachisConsensus<ClusterStateMachine>();
            _engine.Initialize(_env);

            _engine.StateMachine.DatabaseChanged += DatabasesLandlord.ClusterOnDatabaseChanged;

            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            _notificationsStorage.Initialize(_env, ContextPool);
            DatabaseInfoCache.Initialize(_env, ContextPool);

            NotificationCenter.Initialize();
            foreach (var alertRaised in storeAlertForLateRaise)
            {
                NotificationCenter.Add(alertRaised);
            }
            LicenseManager.Initialize(_env, ContextPool);

            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                foreach (var db in _engine.StateMachine.ItemsStartingWith(context, "db/", 0, int.MaxValue))
                {
                    DatabasesLandlord.ClusterOnDatabaseChanged(this, (db.Item1, 0));
                }
            }
            
            Task.Run(ClusterMaintanceSetupTask, ServerShutdown);
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

        public unsafe void PutSecretKey(
            TransactionOperationContext context,
            string name,
            byte[] key,
            bool overwrite = false /*Be careful with this one, overwriting a key might be disastrous*/)
        {
            Debug.Assert(context.Transaction != null);
            if (key.Length != 256 / 8)
                throw new ArgumentException($"Key size must be 256 bits, but was {key.Length * 8}", nameof(key));

            byte[] existingKey;
            try
            {
                existingKey = GetSecretKey(context, name);
            }
            catch (Exception)
            {
                // failure to read the key might be because the user password has changed
                // in this case, we ignore the existance of the key and overwrite it
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
                    if (Sodium.crypto_generichash(pHash, (IntPtr)hashLen, pKey, (ulong)key.Length, null, IntPtr.Zero) != 0)
                        throw new InvalidOperationException("Failed to hash key");

                    Sparrow.Memory.Copy(pHash + hashLen, pKey, key.Length);

                    var entropy = Sodium.GenerateRandomBuffer(256);

                    var protectedData = SecretProtection.Protect(hash, entropy);

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

            var data = SecretProtection.Unprotect(protectedData, entropy);

            var hashLen = Sodium.crypto_generichash_bytes_max();

            fixed (byte* pData = data)
            fixed (byte* pHash = new byte[hashLen])
            {
                try
                {
                    if (Sodium.crypto_generichash(pHash, (IntPtr)hashLen, pData + hashLen, (ulong)(data.Length - hashLen), null, IntPtr.Zero) != 0)
                        throw new InvalidOperationException($"Unable to compute hash for {name}");

                    if (Sodium.sodium_memcmp(pData, pHash, (IntPtr)hashLen) != 0)
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

            var record = Cluster.ReadDatabase(context, name);

            if (record != null)
                throw new InvalidOperationException($"Cannot delete key {name} where there is an existing database that require its usage");

            var tree = context.Transaction.InnerTransaction.CreateTree("SecretKeys");

            tree.Delete(name);
        }

        public async Task<long> DeleteDatabaseAsync(JsonOperationContext context, string db, bool hardDelete, string fromNode)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(DeleteDatabaseCommand),
                [nameof(DeleteDatabaseCommand.DatabaseName)] = db,
                [nameof(DeleteDatabaseCommand.HardDelete)] = hardDelete,
                [nameof(DeleteDatabaseCommand.FromNode)] = fromNode
            }, "del-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }

        public async Task<long> ModifyDatabaseWatchers(
            JsonOperationContext context, 
            string key, 
            BlittableJsonReaderArray watchers)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(ModifyDatabaseWatchersCommand),
                [nameof(ModifyDatabaseWatchersCommand.DatabaseName)] = key,
                [nameof(ModifyDatabaseWatchersCommand.Watchers)] = watchers,
            }, "update-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }

        public async Task<long> ModifyConflictSolverAsync(JsonOperationContext context, string key, 
            BlittableJsonReaderObject solver)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(ModifyConflictSolverCommand),
                [nameof(ModifyConflictSolverCommand.DatabaseName)] = key,
                [nameof(ModifyConflictSolverCommand.Solver)] = solver
            }, "update-conflict-resolver-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }

        public async Task<long> PutValueInClusterAsync(JsonOperationContext context, string key, BlittableJsonReaderObject val)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(PutValueCommand),
                [nameof(PutValueCommand.Name)] = key,
                [nameof(PutValueCommand.Value)] = val,
            }, "put-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }

        public async Task DeleteValueInClusterAsync(JsonOperationContext context, string key)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(DeleteValueCommand),
                [nameof(DeleteValueCommand.Name)] = key,
            }, "delete-cmd"))
            {
                await SendToLeaderAsync(putCmd);
            }
        }

        public async Task<long> ModifyDatabaseExpiration(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(EditExpirationCommand),
                [nameof(EditExpirationCommand.DatabaseName)] = name,
                [nameof(EditExpirationCommand.Configuration)] = configurationJson,
            }, "expiration-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }

        public async Task<long> ModifyDatabasePeriodicBackup(TransactionOperationContext context, string name, BlittableJsonReaderObject configurationJson)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(EditPeriodicBackupCommand),
                [nameof(EditExpirationCommand.DatabaseName)] = name,
                [nameof(EditExpirationCommand.Configuration)] = configurationJson,
            }, "periodic-export-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }
        
        public async Task<long> ModifyDatabaseVersioning(JsonOperationContext context, string databaseName, BlittableJsonReaderObject val)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(EditVersioningCommand),
                [nameof(EditVersioningCommand.Configuration)] = val,
                [nameof(EditVersioningCommand.DatabaseName)] = databaseName,
            }, "versioning-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
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
                        ContextPool
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
                        // intentionally inside the loop, so we get better concurrency overall
                        // since shutting down a database can take a while
                        DatabasesLandlord.UnloadDatabase(db, skipIfActiveInDuration: maxTimeDatabaseCanBeIdle, shouldSkip: database => database.Configuration.Core.RunInMemory);
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

        public async Task<long> WriteDbAsync(TransactionOperationContext context, string dbId, BlittableJsonReaderObject dbDoc, long? etag)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(AddDatabaseCommand),
                [nameof(AddDatabaseCommand.Name)] = dbId,
                [nameof(AddDatabaseCommand.Value)] = dbDoc,
                [nameof(AddDatabaseCommand.Etag)] = etag
            }, "put-cmd"))
            {
                return await SendToLeaderAsync(putCmd);
            }
        }

        public void EnsureNotPassive()
        {
            if (_engine.CurrentState == RachisConsensus.State.Passive)
            {
                _engine.Bootstarp(_ravenServer.WebUrls[0]);
            }
        }

        public Task<long> PutCommandAsync(BlittableJsonReaderObject cmd)
        {
            return _engine.PutAsync(cmd);
        }

        public bool IsLeader()
        {
            return _engine.CurrentState == RachisConsensus.State.Leader;
        }

        public async Task<long> SendToLeaderAsync(UpdateDatabaseCommand cmd)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var djv = cmd.ToJson();
                var cmdJson = context.ReadObject(djv, "raft/command");

                while (true)
                {
                    var logChange = _engine.WaitForHeartbeat();

                    if (_engine.CurrentState == RachisConsensus.State.Leader)
                    {
                        return await _engine.PutAsync(cmdJson);
                    }

                    var engineLeaderTag = _engine.LeaderTag; // not actually working
                    try
                    {
                        return await SendToNodeAsync(context, engineLeaderTag, cmdJson);
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Tried to send message to leader, retrying", ex);
                    }

                    await logChange;
                }
            }
        }

        public async Task<long> SendToLeaderAsync(BlittableJsonReaderObject cmd)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                //TODO: timeout, server shutdown handling, etc
                while (true)
                {
                    var logChange = _engine.WaitForHeartbeat();

                    if (_engine.CurrentState == RachisConsensus.State.Leader)
                    {
                        return await _engine.PutAsync(cmd);
                    }

                    var engineLeaderTag = _engine.LeaderTag; // not actually working
                    try
                    {
                        return await SendToNodeAsync(context, engineLeaderTag, cmd);
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Tried to send message to leader, retrying", ex);
                    }

                    await logChange;
                }
            }
        }

        private async Task<long> SendToNodeAsync(TransactionOperationContext context, string engineLeaderTag, BlittableJsonReaderObject cmd)
        {
            using (context.OpenReadTransaction())
            {
                var clusterTopology = _engine.GetTopology(context);
                string leaderUrl;
                if (clusterTopology.Members.TryGetValue(engineLeaderTag, out leaderUrl) == false)
                    throw new InvalidOperationException("Leader " + engineLeaderTag + " was not found in the topology members");
                var command = new PutRaftCommand(context, cmd);

                if (_clusterRequestExecutor == null)
                    _clusterRequestExecutor = RequestExecutor.CreateForSingleNode(leaderUrl, "Rachis.Server", clusterTopology.ApiKey);
                else if (_clusterRequestExecutor.Url.Equals(leaderUrl, StringComparison.OrdinalIgnoreCase) == false ||
                         _clusterRequestExecutor.ApiKey?.Equals(clusterTopology.ApiKey) == false)
                {
                    _clusterRequestExecutor.Dispose();
                    _clusterRequestExecutor = RequestExecutor.CreateForSingleNode(leaderUrl, "Rachis.Server", clusterTopology.ApiKey);
                }

                await _clusterRequestExecutor.ExecuteAsync(command, context, ServerShutdown);

                return command.Result.ETag;
            }
        }

        protected internal class PutRaftCommand : RavenCommand<PutRaftCommandResult>
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject _command;
            public override bool IsReadRequest => false;
            public long CommandIndex { get; private set; }

            public PutRaftCommand(JsonOperationContext context, BlittableJsonReaderObject command)
            {
                _context = context;
                _command = context.ReadObject(command, "Raft command");
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/rachis/send";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteObject(_command);
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationCluster.PutRaftCommandResult(response);
            }
        }

        public class PutRaftCommandResult
        {
            public long ETag { get; set; }
        }

        public Task WaitForTopology(Leader.TopologyModification state)
        {
            return _engine.WaitForTopology(state);
        }

        public Task WaitForState(RachisConsensus.State state)
        {
            return _engine.WaitForState(state);
        }

        public void ClusterAcceptNewConnection(TcpClient client)
        {
            _engine.AcceptNewConnection(client);
        }

        public async Task WaitForCommitIndexChange(RachisConsensus.CommitIndexModification modification, long value)
        {
            await _engine.WaitForCommitIndexChange(modification, value);
        }

        public string ClusterStatus()
        {
            return _engine.CurrentState + ", " + _engine.LastStateChangeReason;
        }
    }
}