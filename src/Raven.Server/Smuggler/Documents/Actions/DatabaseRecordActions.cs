using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Logging;

namespace Raven.Server.Smuggler.Documents.Actions;

public class DatabaseRecordActions : IDatabaseRecordActions
{
    private readonly ServerStore _server;
    private readonly Logger _log;
    private readonly string _name;
    private readonly DatabaseRecord _currentDatabaseRecord;

    public DatabaseRecordActions(DocumentDatabase database, Logger log)
    {
        _server = database.ServerStore;
        _name = database.Name;
        _currentDatabaseRecord = database.ReadDatabaseRecord();
        _log = log;
    }

    public DatabaseRecordActions(ServerStore server, DatabaseRecord currentDatabaseRecord, string name, Logger log)
    {
        _server = server;
        _currentDatabaseRecord = currentDatabaseRecord;
        _name = name;
        _log = log;
    }

    public async ValueTask WriteDatabaseRecordAsync(DatabaseRecord databaseRecord,
        SmugglerProgressBase.DatabaseRecordProgress progress,
        AuthorizationStatus authorizationStatus,
        DatabaseRecordItemType databaseRecordItemType)
    {
        var tasks = new List<Task<(long Index, object Result)>>();

        if (databaseRecord == null)
            return;

        if (databaseRecord.ConflictSolverConfig != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.ConflictSolverConfig))
        {
            if (_currentDatabaseRecord?.ConflictSolverConfig != null)
            {
                foreach (var collection in _currentDatabaseRecord.ConflictSolverConfig.ResolveByCollection)
                {
                    if (databaseRecord.ConflictSolverConfig.ResolveByCollection.ContainsKey(collection.Key) == false)
                    {
                        databaseRecord.ConflictSolverConfig.ResolveByCollection.Add(collection.Key, collection.Value);
                    }
                }
            }

            if (_log.IsInfoEnabled)
                _log.Info("Configuring conflict solver config from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new ModifyConflictSolverCommand(_name, RaftIdGenerator.DontCareId)
            {
                Solver = databaseRecord.ConflictSolverConfig
            }));
            progress.ConflictSolverConfigUpdated = true;
        }

        if (databaseRecord.PeriodicBackups.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.PeriodicBackups))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring periodic backups configuration from smuggler");
            foreach (var backupConfig in databaseRecord.PeriodicBackups)
            {
                _currentDatabaseRecord?.PeriodicBackups.ForEach(x =>
                {
                    if (x.Name.Equals(backupConfig.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.Backup, _name, RaftIdGenerator.DontCareId)));
                    }
                });

                backupConfig.TaskId = 0;
                backupConfig.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new UpdatePeriodicBackupCommand(backupConfig, _name, RaftIdGenerator.DontCareId)));
            }
            progress.PeriodicBackupsUpdated = true;
        }

        if (databaseRecord.SinkPullReplications.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.SinkPullReplications))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring sink pull replication configuration from smuggler");
            foreach (var pullReplication in databaseRecord.SinkPullReplications)
            {
                _currentDatabaseRecord?.SinkPullReplications.ForEach(x =>
                {
                    if (x.Name.Equals(pullReplication.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.PullReplicationAsSink, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                pullReplication.TaskId = 0;
                pullReplication.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new UpdatePullReplicationAsSinkCommand(_name, RaftIdGenerator.DontCareId)
                {
                    PullReplicationAsSink = pullReplication
                }));
            }
            progress.SinkPullReplicationsUpdated = true;
        }

        if (databaseRecord.HubPullReplications.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.HubPullReplications))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring hub pull replication configuration from smuggler");
            foreach (var pullReplication in databaseRecord.HubPullReplications)
            {
                _currentDatabaseRecord?.HubPullReplications.ForEach(x =>
                {
                    if (x.Name.Equals(pullReplication.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.PullReplicationAsHub, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                pullReplication.TaskId = 0;
                pullReplication.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new UpdatePullReplicationAsHubCommand(_name, RaftIdGenerator.DontCareId)
                    {
                        Definition = pullReplication
                    }
                ));
            }
            progress.HubPullReplicationsUpdated = true;
        }

        if (databaseRecord.Sorters.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Sorters))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring sorters configuration from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new PutSortersCommand(_name, RaftIdGenerator.DontCareId)
            {
                Sorters = databaseRecord.Sorters.Values.ToList()
            }));

            progress.SortersUpdated = true;
        }

        if (databaseRecord.Analyzers.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Analyzers))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring analyzers configuration from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new PutAnalyzersCommand(_name, RaftIdGenerator.DontCareId)
            {
                Analyzers = databaseRecord.Analyzers.Values.ToList()
            }));

            progress.AnalyzersUpdated = true;
        }

        if (databaseRecord.ExternalReplications.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.ExternalReplications))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring external replications configuration from smuggler");
            foreach (var replication in databaseRecord.ExternalReplications)
            {
                _currentDatabaseRecord?.ExternalReplications.ForEach(x =>
                {
                    if (x.Name.Equals(replication.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.Replication, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                replication.TaskId = 0;
                replication.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new UpdateExternalReplicationCommand(_name, RaftIdGenerator.DontCareId)
                {
                    Watcher = replication
                }));
            }
            progress.ExternalReplicationsUpdated = true;
        }

        if (databaseRecord.RavenConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.RavenConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring Raven connection strings configuration from smuggler");
            foreach (var connectionString in databaseRecord.RavenConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutRavenConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }
            progress.RavenConnectionStringsUpdated = true;
        }

        if (databaseRecord.RavenEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.RavenEtls))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring raven etls configuration from smuggler");
            foreach (var etl in databaseRecord.RavenEtls)
            {
                _currentDatabaseRecord?.RavenEtls.ForEach(x =>
                {
                    if (x.Name.Equals(etl.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.RavenEtl, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                etl.TaskId = 0;
                etl.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new AddRavenEtlCommand(etl, _name, RaftIdGenerator.DontCareId)));
            }
            progress.RavenEtlsUpdated = true;
        }

        if (databaseRecord.SqlConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.SqlConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring SQL connection strings from smuggler");
            foreach (var connectionString in databaseRecord.SqlConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutSqlConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }
            progress.SqlConnectionStringsUpdated = true;
        }

        if (databaseRecord.SqlEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.SqlEtls))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring sql etls configuration from smuggler");
            foreach (var etl in databaseRecord.SqlEtls)
            {
                _currentDatabaseRecord?.SqlEtls.ForEach(x =>
                {
                    if (x.Name.Equals(etl.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.SqlEtl, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                etl.TaskId = 0;
                etl.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new AddSqlEtlCommand(etl, _name, RaftIdGenerator.DontCareId)));
            }
            progress.SqlEtlsUpdated = true;
        }

        if (databaseRecord.TimeSeries != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.TimeSeries))
        {
            if (_currentDatabaseRecord?.TimeSeries != null)
            {
                foreach (var collection in _currentDatabaseRecord.TimeSeries.Collections)
                {
                    if (databaseRecord.TimeSeries.Collections.ContainsKey(collection.Key) == false)
                    {
                        databaseRecord.TimeSeries.Collections.Add(collection.Key, collection.Value);
                    }
                }
            }
            if (_log.IsInfoEnabled)
                _log.Info("Configuring time-series from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditTimeSeriesConfigurationCommand(databaseRecord.TimeSeries, _name, RaftIdGenerator.DontCareId)));
            progress.TimeSeriesConfigurationUpdated = true;
        }

        if (databaseRecord.DocumentsCompression != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.DocumentsCompression))
        {
            if (_currentDatabaseRecord?.DocumentsCompression?.Collections?.Length > 0 || _currentDatabaseRecord?.DocumentsCompression?.CompressAllCollections == true)
            {
                var collectionsToAdd = new List<string>();

                foreach (var collection in _currentDatabaseRecord.DocumentsCompression.Collections)
                {
                    if (_currentDatabaseRecord?.DocumentsCompression?.CompressAllCollections == true ||
                        databaseRecord.DocumentsCompression.Collections.Contains(collection) == false)
                    {
                        collectionsToAdd.Add(collection);
                    }
                }

                if (collectionsToAdd.Count > 0)
                {
                    databaseRecord.DocumentsCompression.Collections = collectionsToAdd.Concat(_currentDatabaseRecord.DocumentsCompression.Collections).ToArray();
                }
            }

            if (_log.IsInfoEnabled)
                _log.Info("Configuring documents compression from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditDocumentsCompressionCommand(databaseRecord.DocumentsCompression, _name, RaftIdGenerator.DontCareId)));
            progress.DocumentsCompressionConfigurationUpdated = true;
        }

        if (databaseRecord.Revisions != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Revisions))
        {
            if (_currentDatabaseRecord?.Revisions != null)
            {
                foreach (var collection in _currentDatabaseRecord.Revisions.Collections)
                {
                    if (databaseRecord.Revisions.Collections.ContainsKey(collection.Key) == false)
                    {
                        databaseRecord.Revisions.Collections.Add(collection.Key, collection.Value);
                    }
                }
            }
            if (_log.IsInfoEnabled)
                _log.Info("Configuring revisions from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditRevisionsConfigurationCommand(databaseRecord.Revisions, _name, RaftIdGenerator.DontCareId)));
            progress.RevisionsConfigurationUpdated = true;
        }

        if (databaseRecord.RevisionsForConflicts != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Revisions))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring revisions for conflicts from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditRevisionsForConflictsConfigurationCommand(databaseRecord.RevisionsForConflicts, _name, RaftIdGenerator.DontCareId)));
        }

        if (databaseRecord.Expiration != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Expiration))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring expiration from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditExpirationCommand(databaseRecord.Expiration, _name, RaftIdGenerator.DontCareId)));
            progress.ExpirationConfigurationUpdated = true;
        }

        if (databaseRecord.Refresh != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Expiration))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring refresh from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditRefreshCommand(databaseRecord.Refresh, _name, RaftIdGenerator.DontCareId)));
            progress.RefreshConfigurationUpdated = true;
        }

        if (databaseRecord.Client != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Client))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring client configuration from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new EditDatabaseClientConfigurationCommand(databaseRecord.Client, _name, RaftIdGenerator.DontCareId)));
            progress.ClientConfigurationUpdated = true;
        }

        if (databaseRecord.Integrations?.PostgreSql != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.PostgreSQLIntegration))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring PostgreSQL integration from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditPostgreSqlConfigurationCommand(databaseRecord.Integrations.PostgreSql, _name, RaftIdGenerator.DontCareId)));
            progress.PostreSQLConfigurationUpdated = true;
        }

        if (databaseRecord.UnusedDatabaseIds != null && databaseRecord.UnusedDatabaseIds.Count > 0)
        {
            if (_log.IsInfoEnabled)
                _log.Info("Set unused database Ids from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new UpdateUnusedDatabaseIdsCommand(_name, databaseRecord.UnusedDatabaseIds, RaftIdGenerator.DontCareId)));

            progress.UnusedDatabaseIdsUpdated = true;
        }

        if (databaseRecordItemType.HasFlag(DatabaseRecordItemType.LockMode))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring database lock mode from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new EditLockModeCommand(_name, databaseRecord.LockMode, RaftIdGenerator.DontCareId)));

            progress.LockModeUpdated = true;
        }

        if (databaseRecord.OlapConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.OlapConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring OLAP connection strings from smuggler");
            foreach (var connectionString in databaseRecord.OlapConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutOlapConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }
            progress.OlapConnectionStringsUpdated = true;
        }

        if (databaseRecord.OlapEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.OlapEtls))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring OLAP ETLs configuration from smuggler");
            foreach (var etl in databaseRecord.OlapEtls)
            {
                _currentDatabaseRecord?.OlapEtls.ForEach(x =>
                {
                    if (x.Name.Equals(etl.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.OlapEtl, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                etl.TaskId = 0;
                etl.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new AddOlapEtlCommand(etl, _name, RaftIdGenerator.DontCareId)));
            }
            progress.OlapEtlsUpdated = true;
        }

        if (databaseRecord.ElasticSearchConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.ElasticSearchConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring ElasticSearch connection strings from smuggler");
            foreach (var connectionString in databaseRecord.ElasticSearchConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutElasticSearchConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }
            progress.ElasticSearchConnectionStringsUpdated = true;
        }

        if (databaseRecord.ElasticSearchEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.ElasticSearchEtls))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring ElasticSearch ETLs configuration from smuggler");
            foreach (var etl in databaseRecord.ElasticSearchEtls)
            {
                _currentDatabaseRecord?.ElasticSearchEtls.ForEach(x =>
                {
                    if (x.Name.Equals(etl.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.ElasticSearchEtl, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                etl.TaskId = 0;
                etl.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new AddElasticSearchEtlCommand(etl, _name, RaftIdGenerator.DontCareId)));
            }
            progress.ElasticSearchEtlsUpdated = true;
        }

        if (databaseRecord.QueueConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.QueueConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring Queue ETL connection strings from smuggler");
            foreach (var connectionString in databaseRecord.QueueConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutQueueConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }
            progress.QueueConnectionStringsUpdated = true;
        }

        if (databaseRecord.QueueEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.QueueEtls))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring Queue ETLs configuration from smuggler");
            foreach (var etl in databaseRecord.QueueEtls)
            {
                _currentDatabaseRecord?.QueueEtls.ForEach(x =>
                {
                    if (x.Name.Equals(etl.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.QueueEtl, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                etl.TaskId = 0;
                etl.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new AddQueueEtlCommand(etl, _name, RaftIdGenerator.DontCareId)));
            }
            progress.QueueEtlsUpdated = true;
        }

        if (tasks.Count == 0)
            return;

        long maxIndex = 0;
        foreach (var task in tasks)
        {
            var (index, _) = await task;
            if (index > maxIndex)
                maxIndex = index;
        }

        await _server.Cluster.WaitForIndexNotification(maxIndex, _server.Engine.OperationTimeout);

        tasks.Clear();
    }

    public ValueTask DisposeAsync()
    {
        return default;
    }
}
