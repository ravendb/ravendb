using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Client.Exceptions.Database;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.LowMemoryNotification;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron.Data.Tables;
using Voron.Exceptions;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public class ServerStore : IDisposable
    {
        private readonly CancellationTokenSource _shutdownNotification = new CancellationTokenSource();

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        private static Logger _logger;

        private StorageEnvironment _env;

        private readonly NotificationsStorage _notificationsStorage;



        public readonly RavenConfiguration Configuration;
        private readonly RavenServer _ravenServer;
        public readonly DatabasesLandlord DatabasesLandlord;
        public readonly NotificationCenter.NotificationCenter NotificationCenter;

        public static LicenseStorage LicenseStorage { get; } = new LicenseStorage();

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        public ServerStore(RavenConfiguration configuration, RavenServer ravenServer)
        {
            var resourceName = "ServerStore";

            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            Configuration = configuration;
            _ravenServer = ravenServer;
            _logger = LoggingSource.Instance.GetLogger<ServerStore>(resourceName);
            DatabasesLandlord = new DatabasesLandlord(this);

            _notificationsStorage = new NotificationsStorage(resourceName);

            NotificationCenter = new NotificationCenter.NotificationCenter(_notificationsStorage, resourceName, ServerShutdown);

            DatabaseInfoCache = new DatabaseInfoCache();

            _frequencyToCheckForIdleDatabases = Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;

        }

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public TransactionContextPool ContextPool;

        public ClusterStateMachine Cluster => _engine.StateMachine;

        private Timer _timer;
        private RachisConsensus<ClusterStateMachine> _engine;

        public void Initialize()
        {
            AbstractLowMemoryNotification.Initialize(ServerShutdown, Configuration);

            if (_logger.IsInfoEnabled)
                _logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory.FullPath));

            var path = Configuration.Core.DataDirectory.Combine("System");

            var options = Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(path.FullPath)
                : StorageEnvironmentOptions.ForPath(path.FullPath);

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

            ContextPool = new TransactionContextPool(_env);


            _engine = new RachisConsensus<ClusterStateMachine>();
            _engine.Initialize(_env);

            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            _notificationsStorage.Initialize(_env, ContextPool);
            DatabaseInfoCache.Initialize(_env, ContextPool);
            LicenseStorage.Initialize(_env, ContextPool);
            NotificationCenter.Initialize();
        }

        public async Task PutValueInClusterAsync(JsonOperationContext context, string key, BlittableJsonReaderObject val)
        {
            //TODO: redirect to leader
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(PutValueCommand),
                [nameof(PutValueCommand.Name)] = key,
                [nameof(PutValueCommand.Value)] = val,
            }, "put-cmd"))
            {
                await _engine.PutAsync(putCmd);
            }
        }

        public async Task DeleteValueInClusterAsync(JsonOperationContext context, string key)
        {
            //TODO: redirect to leader
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(DeleteValueCommand),
                [nameof(DeleteValueCommand.Name)] = key,
            }, "delete-cmd"))
            {
                await _engine.PutAsync(putCmd);
            }
        }

        public void Dispose()
        {
            if (_shutdownNotification.IsCancellationRequested)
                return;
            lock (this)
            {
                if (_shutdownNotification.IsCancellationRequested)
                    return;
                _shutdownNotification.Cancel();
                var toDispose = new List<IDisposable>
                {
                    _engine,
                    NotificationCenter,
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


        }

        public void IdleOperations(object state)
        {
            try
            {
                foreach (var db in DatabasesLandlord.ResourcesStoresCache)
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
                        DatabasesLandlord.UnloadResource(db, skipIfActiveInDuration: maxTimeDatabaseCanBeIdle, shouldSkip: database => database.Configuration.Core.RunInMemory);
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

        public async Task<long> TEMP_WriteDbAsync(TransactionOperationContext context, string dbId, BlittableJsonReaderObject dbDoc, long? etag)
        {
            using (var putCmd = context.ReadObject(new DynamicJsonValue
            {
                ["Type"] = nameof(TEMP_SetDatabaseCommand),
                [nameof(TEMP_SetDatabaseCommand.Name)] = dbId,
                [nameof(TEMP_SetDatabaseCommand.Value)] = dbDoc,
                [nameof(TEMP_SetDatabaseCommand.Etag)] = etag,
            }, "put-cmd"))
            {
                return await _engine.PutAsync(putCmd);
            }
        }

        public void EnsureNotPassive()
        {
            if (_engine.CurrentState == RachisConsensus.State.Passive)
            {
                _engine.Bootstarp(_ravenServer.WebUrls[0]);
            }
        }
    }
}