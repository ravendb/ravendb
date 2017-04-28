using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
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
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.LowMemoryNotification;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Sparrow.Logging;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private const string ResourceName = nameof(ServerStore);

        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<ServerStore>(ResourceName);

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
            AbstractLowMemoryNotification.Initialize(ServerShutdown, Configuration);

            if (_logger.IsInfoEnabled)
                _logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

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
                    "NonDurable Error System");
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
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(
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

                    var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(ServerStore)}.");

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
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error during idle operation run for " + db.Key, e);
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
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Error during idle operations for the server", e);
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
                [nameof(AddDatabaseCommand.Etag)] = etag,
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
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Tried to send message to leader, retrying", ex);
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
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Tried to send message to leader, retrying", ex);
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