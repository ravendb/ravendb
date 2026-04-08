using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.CdcSink;

public class CdcSinkLoader : IDisposable
{
    private const string AlertTitle = "CDC Sink loader";

    private CdcSinkProcess[] _processes = [];

    private readonly HashSet<string> _uniqueConfigurationNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    private DatabaseRecord _databaseRecord;
    private readonly object _loadProcessedLock = new object();
    private readonly DocumentDatabase _database;
    private readonly ServerStore _serverStore;
    protected RavenLogger Logger;
    public CdcSinkProcess[] Processes => _processes;

    public event Action<(string ConfigurationName, string TableName, CdcSinkProcessStatistics Statistics)> BatchCompleted;

    public void OnBatchCompleted(string configurationName, string tableName, CdcSinkProcessStatistics statistics)
    {
        BatchCompleted?.Invoke((configurationName, tableName, statistics));
    }

    public event Action<CdcSinkProcess> ProcessAdded;

    public event Action<CdcSinkProcess> ProcessRemoved;

    public List<CdcSinkConfiguration> Sinks;

    public void Initialize(DatabaseRecord record)
    {
        LoadProcesses(record, record.CdcSinks, toRemove: null);
    }

    public CdcSinkLoader() { }

    public CdcSinkLoader(DocumentDatabase documentDatabase, ServerStore serverStore)
    {
        _database = documentDatabase;
        _serverStore = serverStore;
        Logger = documentDatabase.Loggers.GetLogger(GetType());
    }

    private void LoadProcesses(DatabaseRecord record, List<CdcSinkConfiguration> newDestinations,
        List<CdcSinkProcess> toRemove)
    {
        lock (_loadProcessedLock)
        {
            _databaseRecord = record;

            Sinks = _databaseRecord.CdcSinks;

            var processes = new List<CdcSinkProcess>(_processes);

            if (toRemove != null && toRemove.Count > 0)
            {
                foreach (var process in toRemove)
                {
                    processes.Remove(process);
                    _uniqueConfigurationNames.Remove(process.Configuration.Name);

                    OnProcessRemoved(process);
                }
            }

            var ensureUniqueConfigurationNames = new HashSet<string>(_uniqueConfigurationNames, StringComparer.OrdinalIgnoreCase);

            var newProcesses = new List<CdcSinkProcess>();
            if (newDestinations != null && newDestinations.Count > 0)
                newProcesses.AddRange(GetRelevantProcesses(newDestinations, ensureUniqueConfigurationNames));

            processes.AddRange(newProcesses);
            _processes = processes.ToArray();

            foreach (var process in newProcesses)
            {
                process.Start();

                OnProcessAdded(process);

                _uniqueConfigurationNames.Add(process.Configuration.Name);
            }
        }
    }

    private IEnumerable<CdcSinkProcess> GetRelevantProcesses(List<CdcSinkConfiguration> configurations,
        HashSet<string> uniqueNames)
    {
        foreach (var config in configurations)
        {
            var connectionStringNotFound = false;

            if (_databaseRecord.SqlConnectionStrings.TryGetValue(config.ConnectionStringName, out var sqlConnection))
                config.Initialize(sqlConnection);
            else
                connectionStringNotFound = true;

            if (connectionStringNotFound)
            {
                LogConfigurationError(config,
                    new List<string> { $"Connection string named '{config.ConnectionStringName}' was not found." });

                continue;
            }

            if (ValidateConfiguration(config, uniqueNames) == false)
                continue;

            var processState = CdcSinkProcess.GetProcessState(_database, config.Name);
            var whoseTaskIsIt = OngoingTasksUtils.WhoseTaskIsIt(_serverStore, _databaseRecord.Topology, config, processState, _database.NotificationCenter);
            if (whoseTaskIsIt != _serverStore.NodeTag)
                continue;

            var process = CreateProcess(config, _database);
            if (process != null)
                yield return process;
        }
    }

    protected virtual CdcSinkProcess CreateProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
    {
        return configuration.Connection?.FactoryName switch
        {
            "Npgsql" => new PostgresCdcSinkProcess(configuration, database),
            "System.Data.SqlClient" or "Microsoft.Data.SqlClient" => new SqlServerCdcSinkProcess(configuration, database),
            "MySql.Data.MySqlClient" or "MySqlConnector.MySqlConnectorFactory" => new MySqlCdcSinkProcess(configuration, database),
            _ => throw new NotSupportedException($"CDC is not supported for provider '{configuration.Connection?.FactoryName}'")
        };
    }

    private bool ValidateConfiguration(CdcSinkConfiguration config, HashSet<string> uniqueNames)
    {
        if (config.Validate(out List<string> errors) == false)
        {
            LogConfigurationError(config, errors);
            return false;
        }

        if (uniqueNames.Add(config.Name) == false)
        {
            LogConfigurationError(config,
                new List<string> { $"CDC Sink with name '{config.Name}' is already defined" });
            return false;
        }

        return true;
    }

    private void OnProcessRemoved(CdcSinkProcess process)
    {
        ProcessRemoved?.Invoke(process);
    }

    private void OnProcessAdded(CdcSinkProcess process)
    {
        ProcessAdded?.Invoke(process);
    }

    public virtual void Dispose()
    {
        var ea = new ExceptionAggregator(Logger, "Could not dispose CDC Sink loader");

        Parallel.ForEach(_processes, x => ea.Execute(x.Dispose));

        ea.ThrowIfNeeded();
    }

    private bool IsMyTask(DatabaseRecord record, CdcSinkConfiguration config, ref Dictionary<string, string> responsibleNodes)
    {
        var processState = CdcSinkProcess.GetProcessState(_database, config.Name);
        var whoseTaskIsIt = OngoingTasksUtils.WhoseTaskIsIt(_serverStore, record.Topology, config, processState, _database.NotificationCenter);

        responsibleNodes[config.Name] = whoseTaskIsIt;

        return whoseTaskIsIt == _serverStore.NodeTag;
    }



    private void LogConfigurationError(CdcSinkConfiguration config, List<string> errors)
    {
        var errorMessage =
            $"Invalid CDC Sink configuration for '{config.Name}'{(config.Connection != null ? $" ({config.GetDestination()})" : string.Empty)}. " +
            $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

        if (Logger.IsInfoEnabled)
            Logger.Info(errorMessage);

        var alert = AlertRaised.Create(_database.Name, AlertTitle, errorMessage, AlertReason.CdcSink_Error, NotificationSeverity.Error);

        _database.NotificationCenter.Add(alert);
    }

    private static string GetStopReason(
        CdcSinkProcess process,
        DatabaseRecord record,
        List<CdcSinkConfiguration> myCdcSinks,
        Dictionary<string, string> responsibleNodes)
    {
        CdcSinkConfigurationCompareDifferences? differences = null;
        var transformationDiffs =
            new List<(string TransformationName, CdcSinkConfigurationCompareDifferences Difference)>();

        var reason = "Database record change. ";

        CdcSinkConfiguration existing = null;
        foreach (var x in myCdcSinks)
        {
            if (x.Name.Equals(process.Configuration.Name, StringComparison.OrdinalIgnoreCase))
            {
                existing = x;
                break;
            }
        }

        if (existing != null)
            differences = process.Configuration.Compare(existing, record.SqlConnectionStrings, transformationDiffs);

        if (differences != null)
        {
            reason += $"Configuration changes: {differences}. Details: ";

            foreach (var transformationDiff in transformationDiffs)
            {
                reason += $"Script '{transformationDiff.TransformationName}' - {transformationDiff.Difference}. ";
            }
        }
        else
        {
            if (responsibleNodes.TryGetValue(process.Configuration.Name, out var responsibleNode))
            {
                reason += $"CDC Sink was moved to another node. Responsible node is: {responsibleNode}";
            }
            else
            {
                reason +=
                    $"CDC Sink was deleted or moved to another node (no configuration named '{process.Configuration.Name}' was found). ";
            }
        }

        return reason;
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        var myCdcSinks = new List<CdcSinkConfiguration>();
        var responsibleNodes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var config in record.CdcSinks)
        {
            if (IsMyTask(record, config, ref responsibleNodes))
            {
                myCdcSinks.Add(config);
            }
        }

        var processesPerConfig = new Dictionary<string, List<CdcSinkProcess>>(StringComparer.OrdinalIgnoreCase);
        foreach (var process in _processes)
        {
            if (processesPerConfig.TryGetValue(process.Configuration.Name, out var list) == false)
            {
                list = new List<CdcSinkProcess>();
                processesPerConfig[process.Configuration.Name] = list;
            }
            list.Add(process);
        }

        var unchangedConfigs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (configName, processList) in processesPerConfig)
        {
            var process = processList[0];

            CdcSinkConfiguration existing = null;

            foreach (var config in myCdcSinks)
            {
                var diff = process.Configuration.Compare(config, record.SqlConnectionStrings);

                if (diff == CdcSinkConfigurationCompareDifferences.None)
                {
                    existing = config;
                    break;
                }
            }

            if (existing != null)
            {
                unchangedConfigs.Add(configName);
                myCdcSinks.Remove(existing);
            }
        }

        var toRemoveList = new List<CdcSinkProcess>();
        foreach (var (configName, processList) in processesPerConfig)
        {
            if (unchangedConfigs.Contains(configName))
                continue;
            for (int i = 0; i < processList.Count; i++)
                toRemoveList.Add(processList[i]);
        }

        // Stop old processes BEFORE starting new ones. PostgreSQL replication slots
        // allow only one consumer, so the old process must release its connection
        // before the replacement can connect.
        if (toRemoveList.Count > 0)
        {
            Parallel.ForEach(toRemoveList, process =>
            {
                try
                {
                    if (_database.DatabaseShutdown.IsCancellationRequested)
                        return;

                    using (process)
                    {
                        string reason = GetStopReason(process, record, myCdcSinks, responsibleNodes);
                        process.Stop(reason);
                    }
                }
                catch (ObjectDisposedException)
                {
                }
                catch (Exception e)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error(
                            $"Failed to dispose CDC sink process {process.Name} on the database record change", e);
                }
            });
        }

        LoadProcesses(record, myCdcSinks, toRemoveList);
    }
}
