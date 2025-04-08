using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Backups.Policies.Database;
using Raven.Server.ServerWide.Backups.Policies.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Exception = System.Exception;

namespace Raven.Server.ServerWide.Backups;

public class ServerBackupRunner : IDisposable
{
    public const int MaxDecisionLogSize = 32;

    private readonly ServerStore _serverStore;
    private PoolOfThreads.LongRunningWork _thread;

    private readonly ConcurrentQueue<DatabaseBackupState> _backupQueue = new();

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DatabaseBackupState>> _backupsPerDatabasePerTaskId = new();

    private List<IServerBackupPolicy> _serverBackupPolicies =
    [
        ServerStartupPolicy.Instance,
        ServerCpuCreditsPolicy.Instance,
        ServerLowMemoryPolicy.Instance,
        ServerHighDirtyMemoryPolicy.Instance
    ];

    private List<IDatabaseBackupPolicy> _databaseBackupPolicies =
    [
        DatabaseExistsPolicy.Instance,
        BackupDisabledPolicy.Instance,
        BackupShouldRunOnThisNodePolicy.Instance
    ];

    private readonly RavenLogger _logger;

    public List<(DateTime Time, string Reason)> DecisionLog { get; } = new();

    public class DatabaseBackupState
    {
        public readonly string DatabaseName;

        public PeriodicBackupConfiguration Configuration { get; private set; }

        public NextBackup NextBackup { get; set; }

        public bool Stale { get; set; }

        public bool Running { get; set; }

        public List<(DateTime Time, string Reason)> DecisionLog { get; } = new();

        public DatabaseBackupState([NotNull] string databaseName, [NotNull] PeriodicBackupConfiguration configuration)
        {
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public void UpdateWith(PeriodicBackupConfiguration newConfiguration)
        {
            if (Configuration.HasBackupFrequencyChanged(newConfiguration))
            {
                NextBackup = null;
            }

            Configuration = newConfiguration;
        }

        public override string ToString()
        {
            return $"'{Configuration.Name} ({Configuration.TaskId})' for database '{DatabaseName}'";
        }

        public void AddToDecisionLog(string reason, DateTime now)
        {
            DecisionLog.Insert(0, (now, reason));

            if (DecisionLog.Count > MaxDecisionLogSize)
                DecisionLog.RemoveAt(DecisionLog.Count - 1);
        }
    }

    public ServerBackupRunner(ServerStore serverStore)
    {
        _serverStore = serverStore;
        _logger = RavenLogManager.Instance.GetLoggerForServer<ServerBackupRunner>();
    }

    public void Initialize()
    {
        _serverBackupPolicies =
        [
            ServerStartupPolicy.Instance,
            ServerCpuCreditsPolicy.Instance,
            ServerLowMemoryPolicy.Instance,
            ServerHighDirtyMemoryPolicy.Instance,
            ClusterHealthPolicy.Instance,
            new ServerConcurrentBackupPolicy(_serverStore.ConcurrentBackupsCounter)
        ];

        _databaseBackupPolicies =
        [
            BackupRunningPolicy.Instance,
            BackupDisabledPolicy.Instance,
            DatabaseExistsPolicy.Instance,
            BackupShouldRunOnThisNodePolicy.Instance,
            BackupTimePolicy.Instance,
            DatabaseLoadedPolicy.Instance
        ];

        using (_serverStore.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var databaseRecord in _serverStore.Cluster.GetAllRawDatabases(context))
            {
                foreach (var periodicBackup in databaseRecord.PeriodicBackups)
                {
                    var backupState = new DatabaseBackupState(databaseRecord.DatabaseName, periodicBackup);

                    RegisterNewBackup(backupState);
                }
            }
        }

        _thread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(Run, null, ThreadNames.ForServerBackupRunner());
    }

    public IReadOnlyList<(DateTime Time, string Reason)> GetDecisionLog() => DecisionLog;

    public IReadOnlyList<(DateTime Time, string Reason)> GetDecisionLogFor(string databaseName, long taskId)
    {
        if (_backupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) && backupsPerDatabasePerTaskId.TryGetValue(taskId, out var backupState))
            return backupState.DecisionLog;

        return null;
    }

    private void RegisterNewBackup(DatabaseBackupState backupState)
    {
        if (_backupsPerDatabasePerTaskId.TryGetValue(backupState.DatabaseName, out var backupsPerDatabasePerTaskId) == false)
            _backupsPerDatabasePerTaskId[backupState.DatabaseName] = backupsPerDatabasePerTaskId = new ConcurrentDictionary<long, DatabaseBackupState>();

        backupsPerDatabasePerTaskId.TryAdd(backupState.Configuration.TaskId, backupState);

        _backupQueue.Enqueue(backupState);
    }

    private void Run(object state)
    {
        try
        {
            while (_serverStore.ServerShutdown.IsCancellationRequested == false)
            {
                DatabaseBackupState firstState = null;
                var now = _serverStore.Server.Time.GetUtcNow();

                while (_backupQueue.Count > 0 &&
                       _backupQueue.TryPeek(out var backupState) &&
                       backupState != firstState &&
                       AreAllServerPoliciesCompliant(now) &&
                       _backupQueue.TryDequeue(out backupState))
                {
                    if (backupState.Stale) // cleanup
                        continue;

                    firstState ??= backupState;

                    bool areAllDatabasePoliciesCompliant;

                    try
                    {
                        areAllDatabasePoliciesCompliant = AreAllDatabasePoliciesCompliant(backupState, now);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsWarnEnabled)
                            _logger.Warn($"Could not process backup policies for {backupState}.", e);

                        areAllDatabasePoliciesCompliant = false;
                    }

                    if (areAllDatabasePoliciesCompliant)
                        RunBackup(backupState);

                    _backupQueue.Enqueue(backupState); // so it will go to the end of the queue which is useful when max number of concurrent backups is set
                }

                _serverStore.ServerShutdown.WaitHandle.WaitOne(_serverStore.Configuration.Backup.BackupRunnerFrequency.AsTimeSpan);
            }
        }
        catch (Exception e)
        {
            // log + notification
        }
    }

    private void RunBackup(DatabaseBackupState backupState)
    {
        ForTestingPurposes?.OnBeforeBackupStarted?.Invoke(backupState);

        _serverStore.ConcurrentBackupsCounter.StartBackup(backupState.DatabaseName, backupState.Configuration.Name, null); // TODO [ppekrol]

        backupState.Running = true;

        _ = Task.Factory.StartNew(async () =>
        {
            try
            {
                var result = _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(backupState.DatabaseName);

                DocumentDatabase database;
                switch (result.DatabaseStatus)
                {
                    case DatabasesLandlord.DatabaseSearchResult.Status.Missing:
                        return;
                    case DatabasesLandlord.DatabaseSearchResult.Status.Database:
                        database = await result.DatabaseTask;
                        break;
                    case DatabasesLandlord.DatabaseSearchResult.Status.Sharded:
                        throw new NotSupportedException("Sharded backup is not supported yet.");
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                var operationId = database.Operations.GetNextOperationId();
                var token = new OperationCancelToken(_serverStore.ServerShutdown);

                var task = database.Operations.AddLocalOperation(
                    operationId,
                    OperationType.DatabaseBackup,
                    $"Periodic Backup '{backupState.Configuration.Name} ({backupState.Configuration.TaskId})' for database '{backupState.DatabaseName}'.",
                    detailedDescription: null,
                    taskFactory: onProgress => StartBackupThread(database, backupState, token, onProgress),
                    token: token);

                _ = task.ContinueWith(_ =>
                {
                    FinishBackup();

                    ForTestingPurposes?.OnAfterBackupCompleted?.Invoke(backupState);

                    token.Dispose();
                });
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Could not start backup for {backupState}.", e);

                FinishBackup();
            }
        });

        return;

        void FinishBackup()
        {
            backupState.NextBackup = null;
            backupState.Running = false;

            _serverStore.ConcurrentBackupsCounter.FinishBackup(backupState.DatabaseName, backupState.Configuration.Name, null, null, null);
        }
    }

    private Task<IOperationResult> StartBackupThread(DocumentDatabase database, DatabaseBackupState backupState, OperationCancelToken token, Action<IOperationProgress> onProgress)
    {
        return Task.FromResult((IOperationResult)null); // TODO [ppekrol] implement backup thread
    }

    private bool AreAllServerPoliciesCompliant(DateTime now)
    {
        foreach (var backupPolicy in _serverBackupPolicies)
        {
            if (backupPolicy.CanDoBackup(_serverStore, now, out string reason) == false)
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug(reason);

                AddToDecisionLog(reason, now);

                ForTestingPurposes?.OnServerPolicyViolation?.Invoke(backupPolicy, reason);

                return false;
            }
        }

        return true;
    }

    private bool AreAllDatabasePoliciesCompliant(DatabaseBackupState backupState, DateTime now)
    {
        using (_serverStore.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var backupPolicy in _databaseBackupPolicies)
            {
                if (backupPolicy.CanDoBackup(context, _serverStore, backupState, now, out string reason) == false)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug(reason);

                    backupState.AddToDecisionLog(reason, now);

                    ForTestingPurposes?.OnDatabasePolicyViolation?.Invoke(backupPolicy, reason, backupState);

                    return false;
                }
            }

            return true;
        }
    }

    public void Dispose()
    {
        _thread.Join(int.MaxValue); // TODO [ppekrol]
    }

    public void HandleDatabaseRecordChange(RawDatabaseRecord databaseRecord)
    {
        if (databaseRecord.PeriodicBackups == null || databaseRecord.PeriodicBackups.Count == 0)
        {
            if (_backupsPerDatabasePerTaskId.TryGetValue(databaseRecord.DatabaseName, out var backupsPerDatabasePerTaskId))
            {
                foreach (var kvp in backupsPerDatabasePerTaskId.ForceEnumerateInThreadSafeManner())
                {
                    kvp.Value.Stale = true;
                }

                // queue will clear itself
                backupsPerDatabasePerTaskId.Clear();
            }

            return;
        }

        if (_backupsPerDatabasePerTaskId.TryGetValue(databaseRecord.DatabaseName, out var backupsPerDatabase) == false)
            _backupsPerDatabasePerTaskId[databaseRecord.DatabaseName] = backupsPerDatabase = new ConcurrentDictionary<long, DatabaseBackupState>();

        foreach (var periodicBackup in databaseRecord.PeriodicBackups)
        {
            if (backupsPerDatabase.TryGetValue(periodicBackup.TaskId, out var currentBackupState))
                currentBackupState.UpdateWith(periodicBackup);
            else
            {
                var newBackupState = new DatabaseBackupState(databaseRecord.DatabaseName, periodicBackup);

                RegisterNewBackup(newBackupState);
            }
        }

        foreach (var removedTaskId in backupsPerDatabase.Keys.Except(databaseRecord.PeriodicBackupsTaskIds))
        {
            if (backupsPerDatabase.TryRemove(removedTaskId, out var backupState))
                backupState.Stale = true;
        }
    }

    public void AddToDecisionLog(string reason, DateTime now)
    {
        DecisionLog.Insert(0, (now, reason));

        if (DecisionLog.Count > MaxDecisionLogSize)
            DecisionLog.RemoveAt(DecisionLog.Count - 1);
    }

    internal TestingStuff ForTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (ForTestingPurposes != null)
            return ForTestingPurposes;

        return ForTestingPurposes = new TestingStuff();
    }

    internal class TestingStuff
    {
        public Action<IDatabaseBackupPolicy, string, DatabaseBackupState> OnDatabasePolicyViolation { get; set; }

        public Action<IServerBackupPolicy, string> OnServerPolicyViolation { get; set; }

        public Action<DatabaseBackupState> OnBeforeBackupStarted { get; set; }

        public Action<DatabaseBackupState> OnAfterBackupCompleted { get; set; }
    }
}
