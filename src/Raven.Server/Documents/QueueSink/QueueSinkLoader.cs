using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.QueueSink;

public class QueueSinkLoader : IDisposable
{
    private const string AlertTitle = "Queue Sink loader";
    
    private QueueSinkProcess[] _processes = new QueueSinkProcess[0];

    private readonly HashSet<string> _uniqueConfigurationNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    private DatabaseRecord _databaseRecord;
    private readonly object _loadProcessedLock = new object();
    private readonly DocumentDatabase _database;
    private readonly ServerStore _serverStore;
    protected Logger Logger;
    public QueueSinkProcess[] Processes => _processes;
    
    public event Action<(string ConfigurationName, string TransformationName, QueueSinkProcessStatistics Statistics)> BatchCompleted;

    public void OnBatchCompleted(string configurationName, string transformationName, QueueSinkProcessStatistics statistics)
    {
        BatchCompleted?.Invoke((configurationName, transformationName, statistics));
    }

    public event Action<QueueSinkProcess> ProcessAdded;

    public event Action<QueueSinkProcess> ProcessRemoved;

    public void Initialize(DatabaseRecord record)
    {
        LoadProcesses(record, record.QueueSinks, toRemove: null);
    }

    public QueueSinkLoader() { }

    public QueueSinkLoader(DocumentDatabase documentDatabase, ServerStore serverStore)
    {
        _database = documentDatabase;
        _serverStore = serverStore;
        Logger = LoggingSource.Instance.GetLogger(documentDatabase.Name, GetType().FullName);
    }

    private void LoadProcesses(DatabaseRecord record, List<QueueSinkConfiguration> newQueueSinkDestinations,
        List<QueueSinkProcess> toRemove)
    {
        lock (_loadProcessedLock)
        {
            _databaseRecord = record;
            var processes = new List<QueueSinkProcess>(_processes);

            if (toRemove != null && toRemove.Count > 0)
            {
                foreach (var process in toRemove)
                {
                    processes.Remove(process);
                    _uniqueConfigurationNames.Remove(process.Configuration.Name);

                    OnProcessRemoved(process);
                }
            }

            var ensureUniqueConfigurationNames = _uniqueConfigurationNames.ToHashSet(StringComparer.OrdinalIgnoreCase);

            var newProcesses = new List<QueueSinkProcess>();
            if (newQueueSinkDestinations != null && newQueueSinkDestinations.Count > 0)
                newProcesses.AddRange(
                    GetRelevantProcesses<QueueSinkConfiguration, QueueConnectionString>(newQueueSinkDestinations,
                        ensureUniqueConfigurationNames));

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

    private IEnumerable<QueueSinkProcess> GetRelevantProcesses<T, TConnectionString>(List<T> configurations,
        HashSet<string> uniqueNames) where T : QueueSinkConfiguration where TConnectionString : ConnectionString
    {
        foreach (var config in configurations)
        {
            var connectionStringNotFound = false;

            QueueSinkConfiguration queueSinkConfig = config;
            if (_databaseRecord.QueueConnectionStrings.TryGetValue(config.ConnectionStringName, out var queueConnection))
                queueSinkConfig.Initialize(queueConnection);
            else
                connectionStringNotFound = true;

            if (connectionStringNotFound)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        $"Connection string named '{config.ConnectionStringName}' was not found."
                    });

                continue;
            }

            if (ValidateConfiguration(config, uniqueNames) == false)
                continue;

            var processState = GetProcessState(config.Scripts, _database, config.Name);
            var whoseTaskIsIt = BackupUtils.WhoseTaskIsIt(_serverStore, _databaseRecord.Topology, config, processState,
                _database.NotificationCenter);
            if (whoseTaskIsIt != _serverStore.NodeTag)
                continue;

            foreach (var transform in config.Scripts)
            {
                QueueSinkProcess process = QueueSinkProcess.CreateInstance(transform, config, _database);
                yield return process;
            }
        }
    }
    
    private bool ValidateConfiguration(QueueSinkConfiguration config, HashSet<string> uniqueNames)
        {
            if (config.Validate(out List<string> errors) == false)
            {
                LogConfigurationError(config, errors);
                return false;
            }

            if (uniqueNames.Add(config.Name) == false)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        $"Queue Sink with name '{config.Name}' is already defined"
                    });
                return false;
            }

            return true;
        }

    private void OnProcessRemoved(QueueSinkProcess process)
    {
        ProcessRemoved?.Invoke(process);
    }

    private void OnProcessAdded(QueueSinkProcess process)
    {
        ProcessAdded?.Invoke(process);
    }

    public virtual void Dispose()
    {
        var ea = new ExceptionAggregator(Logger, "Could not dispose Queue Sink loader");

        Parallel.ForEach(_processes, x => ea.Execute(x.Dispose));

        ea.ThrowIfNeeded();
    }

    private bool IsMyQueueSinkTask<T, TConnectionString>(DatabaseRecord record, T queueSinkTask,
        ref Dictionary<string, string> responsibleNodes)
        where TConnectionString : ConnectionString
        where T : QueueSinkConfiguration
    {
        var processState = GetProcessState(queueSinkTask.Scripts, _database, queueSinkTask.Name);
        var whoseTaskIsIt = BackupUtils.WhoseTaskIsIt(_serverStore, record.Topology, queueSinkTask, processState,
            _database.NotificationCenter);

        responsibleNodes[queueSinkTask.Name] = whoseTaskIsIt;

        return whoseTaskIsIt == _serverStore.NodeTag;
    }

    public static QueueSinkProcessState GetProcessState(List<QueueSinkScript> scripts, DocumentDatabase database,
        string configurationName)
    {
        QueueSinkProcessState processState = null;

        foreach (var script in scripts)
        {
            if (script.Name == null)
                continue;

            processState = QueueSinkProcess.GetProcessState(database, configurationName, script.Name);
            if (processState.NodeTag != null)
                break;
        }

        return processState ?? new QueueSinkProcessState();
    }
    
    private void LogConfigurationError(QueueSinkConfiguration config, List<string> errors)
    {
        var errorMessage =
            $"Invalid Queue Sink configuration for '{config.Name}'{(config.Connection != null ? $" ({config.GetDestination()})" : string.Empty)}. " +
            $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

        if (Logger.IsInfoEnabled)
            Logger.Info(errorMessage);

        var alert = AlertRaised.Create(_database.Name, AlertTitle, errorMessage, AlertType.QueueSink_Error, NotificationSeverity.Error);

        _database.NotificationCenter.Add(alert);
    }

    private static string GetStopReason(QueueSinkProcess process, List<QueueSinkConfiguration> myQueueSink,
        Dictionary<string, string> responsibleNodes)
    {
        QueueSinkConfigurationCompareDifferences? differences = null;
        var transformationDiffs =
            new List<(string TransformationName, QueueSinkConfigurationCompareDifferences Difference)>();

        var reason = "Database record change. ";

        if (process is not null)
        {
            var existing = myQueueSink.FirstOrDefault(x =>
                x.Name.Equals(process.Configuration.Name, StringComparison.OrdinalIgnoreCase));

            if (existing != null)
                differences = process.Configuration.Compare(existing, transformationDiffs);
        }
        else
        {
            throw new InvalidOperationException($"Unknown Queue Sink process type: " + process.GetType().FullName);
        }

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
                reason += $"Queue Sink was moved to another node. Responsible node is: {responsibleNode}";
            }
            else
            {
                reason +=
                    $"Queue Sink was deleted or moved to another node (no configuration named '{process.Configuration.Name}' was found). ";
            }
        }

        return reason;
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        var myQueueSink = new List<QueueSinkConfiguration>();
        var responsibleNodes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var config in record.QueueSinks)
        {
            if (IsMyQueueSinkTask<QueueSinkConfiguration, QueueConnectionString>(record, config, ref responsibleNodes))
            {
                myQueueSink.Add(config);
            }
        }

        var toRemove = _processes.GroupBy(x => x.Configuration.Name).ToDictionary(x => x.Key, x => x.ToList());

        foreach (var processesPerConfig in _processes.GroupBy(x => x.Configuration.Name))
        {
            var process = processesPerConfig.First();

            Debug.Assert(processesPerConfig.All(x => x.GetType() == process.GetType()));
            
            QueueSinkConfiguration existing = null;

            foreach (var config in myQueueSink)
            {
                var diff = process.Configuration.Compare(config);

                if (diff == QueueSinkConfigurationCompareDifferences.None && process.Configuration.Equals(config))
                {
                    existing = config;
                    break;
                }
            }

            if (existing != null)
            {
                toRemove.Remove(processesPerConfig.Key);
                myQueueSink.Remove(existing);
            }
        }

        Parallel.ForEach(toRemove, x =>
        {
            foreach (var process in x.Value)
            {
                _database.DatabaseShutdown.ThrowIfCancellationRequested();

                try
                {
                    string reason = GetStopReason(process, myQueueSink, responsibleNodes);
                    process.Stop(reason);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to stop Queue Sink process {process.Name} on the database record change", e);
                }
            }
        });

        LoadProcesses(record, myQueueSink, toRemove.SelectMany(x => x.Value).ToList());

        Parallel.ForEach(toRemove, x =>
        {
            foreach (var process in x.Value)
            {
                _database.DatabaseShutdown.ThrowIfCancellationRequested();

                try
                {
                    process.Dispose();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info(
                            $"Failed to dispose queue sink process {process.Name} on the database record change", e);
                }
            }
        });
    }
}
