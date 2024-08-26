using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Logging;

namespace Raven.Server.Smuggler.Documents.Actions;

public sealed class DatabaseRecordActions : IDatabaseRecordActions
{
    private readonly ServerStore _server;
    private readonly RavenLogger _log;
    private readonly string _name;
    private readonly DatabaseRecord _currentDatabaseRecord;

    public DatabaseRecordActions(DocumentDatabase database, RavenLogger log)
    {
        _server = database.ServerStore;
        _name = database.Name;
        _currentDatabaseRecord = database.ReadDatabaseRecord();
        _log = log;
    }

    public DatabaseRecordActions(ServerStore server, DatabaseRecord currentDatabaseRecord, string name, RavenLogger log)
    {
        _server = server;
        _currentDatabaseRecord = currentDatabaseRecord;
        _name = name;
        _log = log;
    }

    public async ValueTask WriteDatabaseRecordAsync(DatabaseRecord databaseRecord, SmugglerResult result, AuthorizationStatus authorizationStatus,
        DatabaseRecordItemType databaseRecordItemType)
    {
        var tasks = new List<Task<(long Index, object Result)>>();

        if (databaseRecord == null)
            return;

        var configuration = DatabasesLandlord.CreateDatabaseConfiguration(_server, _name, databaseRecord.Settings);
        var authenticationEnabled = _server.Configuration.Security.AuthenticationEnabled;

        if (databaseRecord.ConflictSolverConfig != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.ConflictSolverConfig))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring conflict solver config from smuggler");

            if (_currentDatabaseRecord?.ConflictSolverConfig != null)
            {
                foreach (var collection in _currentDatabaseRecord.ConflictSolverConfig.ResolveByCollection)
                {
                    if (databaseRecord.ConflictSolverConfig.ResolveByCollection.TryGetValue(collection.Key, out var collectionConfiguration) == false)
                    {
                        databaseRecord.ConflictSolverConfig.ResolveByCollection.Add(collection.Key, collection.Value);
                    }
                    else
                    {
                        if (collectionConfiguration.Equals(collection.Value) == false)
                            result.AddWarning($"Conflict solver configuration of collection '{collection.Key}' already exist on the destination Database Record. " +
                                              "Configuring this conflict solver from smuggler was skipped, even though the configuration differed from the configuration in the target database record");
                    }
                }
            }

            tasks.Add(_server.SendToLeaderAsync(new ModifyConflictSolverCommand(_name, RaftIdGenerator.DontCareId)
            {
                Solver = databaseRecord.ConflictSolverConfig
            }));
            result.DatabaseRecord.ConflictSolverConfigUpdated = true;
        }

        if (databaseRecord.PeriodicBackups.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.PeriodicBackups))
        {
            if (authenticationEnabled && CanAccess(authorizationStatus) == false)
            {
                result.AddError("Import of periodic backup was skipped due to insufficient permissions on your current certificate.");
                result.DatabaseRecord.ErroredCount++;
            }
            else
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
                result.DatabaseRecord.PeriodicBackupsUpdated = true;
            }
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

            result.DatabaseRecord.SinkPullReplicationsUpdated = true;
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

            result.DatabaseRecord.HubPullReplicationsUpdated = true;
        }

        if (databaseRecord.Sorters.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Sorters))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring sorters configuration from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new PutSortersCommand(_name, RaftIdGenerator.DontCareId)
            {
                Sorters = databaseRecord.Sorters.Values.ToList()
            }));

            result.DatabaseRecord.SortersUpdated = true;
        }

        if (databaseRecord.Analyzers.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Analyzers))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring analyzers configuration from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new PutAnalyzersCommand(_name, RaftIdGenerator.DontCareId)
            {
                Analyzers = databaseRecord.Analyzers.Values.ToList()
            }));

            result.DatabaseRecord.AnalyzersUpdated = true;
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

            result.DatabaseRecord.ExternalReplicationsUpdated = true;
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

            result.DatabaseRecord.RavenEtlsUpdated = true;
        }

        if (databaseRecord.SqlEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.SqlEtls))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring SQL ETLs configuration from smuggler");

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

            result.DatabaseRecord.SqlEtlsUpdated = true;
        }

        if (databaseRecord.TimeSeries != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.TimeSeries))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring time-series from smuggler");

            if (_currentDatabaseRecord?.TimeSeries != null)
            {
                foreach (var collection in _currentDatabaseRecord.TimeSeries.Collections)
                {
                    if (databaseRecord.TimeSeries.Collections.TryGetValue(collection.Key, out var collectionConfiguration) == false)
                    {
                        databaseRecord.TimeSeries.Collections.Add(collection.Key, collection.Value);
                    }
                    else
                    {
                        if (collectionConfiguration.Equals(collection.Value) == false)
                            result.AddWarning($"Time-series configuration of collection '{collection.Key}' already exist on the destination Database Record. " +
                                              "Configuring this time-series from smuggler was skipped, even though the configuration differed from the configuration in the target database record");
                    }
                }
            }

            tasks.Add(_server.SendToLeaderAsync(new EditTimeSeriesConfigurationCommand(databaseRecord.TimeSeries, _name, RaftIdGenerator.DontCareId)));
            result.DatabaseRecord.TimeSeriesConfigurationUpdated = true;
        }

        if (databaseRecord.DocumentsCompression != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.DocumentsCompression))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring documents compression from smuggler");

            if (_currentDatabaseRecord?.DocumentsCompression?.Collections?.Length > 0 || _currentDatabaseRecord?.DocumentsCompression?.CompressAllCollections == true)
            {
                var collectionsToAdd = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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

            tasks.Add(_server.SendToLeaderAsync(new EditDocumentsCompressionCommand(databaseRecord.DocumentsCompression, _name, RaftIdGenerator.DontCareId)));
            result.DatabaseRecord.DocumentsCompressionConfigurationUpdated = true;
        }

        if (databaseRecord.Revisions != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Revisions))
        {
            if (authenticationEnabled && CanAccess(authorizationStatus) == false)
            {
                result.AddError("Import of Revision configuration was skipped due to insufficient permissions on your current certificate.");
                result.DatabaseRecord.ErroredCount++;
            }
            else
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Configuring revisions from smuggler");

                if (_currentDatabaseRecord?.Revisions != null)
                {
                    foreach (var collection in _currentDatabaseRecord.Revisions.Collections)
                    {
                        if (databaseRecord.Revisions.Collections.TryGetValue(collection.Key, out var collectionConfiguration) == false)
                        {
                            databaseRecord.Revisions.Collections.Add(collection.Key, collection.Value);
                        }
                        else
                        {
                            if (collectionConfiguration.Equals(collection.Value) == false)
                                result.AddWarning($"Revisions configuration of collection '{collection.Key}' already exist on the destination Database Record. " +
                                                  "Configuring this revisions from smuggler was skipped, even though the configuration differed from the configuration in the target database record");
                        }
                    }
                }

                tasks.Add(_server.SendToLeaderAsync(new EditRevisionsConfigurationCommand(databaseRecord.Revisions, _name, RaftIdGenerator.DontCareId)));
                result.DatabaseRecord.RevisionsConfigurationUpdated = true;
            }
        }

        if (databaseRecord.RevisionsForConflicts != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Revisions))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring revisions for conflicts from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditRevisionsForConflictsConfigurationCommand(databaseRecord.RevisionsForConflicts, _name, RaftIdGenerator.DontCareId)));
        }

        if (databaseRecord.Expiration != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Expiration))
        {
            if (authenticationEnabled && CanAccess(authorizationStatus) == false)
            {
                result.AddError("Import of Expiration was skipped due to insufficient permissions on your current certificate.");
                result.DatabaseRecord.ErroredCount++;
            }
            else
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Configuring expiration from smuggler");

                tasks.Add(_server.SendToLeaderAsync(new EditExpirationCommand(databaseRecord.Expiration, _name, RaftIdGenerator.DontCareId)));
                result.DatabaseRecord.ExpirationConfigurationUpdated = true;
            }
        }

        if (databaseRecord.Refresh != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Refresh))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring refresh from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new EditRefreshCommand(databaseRecord.Refresh, _name, RaftIdGenerator.DontCareId)));
            result.DatabaseRecord.RefreshConfigurationUpdated = true;
        }
        
        if (databaseRecord.DataArchival != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.DataArchival))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring Data Archival from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new EditDataArchivalCommand(databaseRecord.DataArchival, _name, RaftIdGenerator.DontCareId)));
            result.DatabaseRecord.DataArchivalConfigurationUpdated = true;
        }

        if (databaseRecord.RavenConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.RavenConnectionStrings))
        {
            if (authenticationEnabled && CanAccess(authorizationStatus) == false)
            {
                result.AddError("Import of Raven Connection Strings was skipped due to insufficient permissions on your current certificate.");
                result.DatabaseRecord.ErroredCount++;
            }
            else
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Configuring Raven connection strings configuration from smuggler");

                foreach (var connectionString in databaseRecord.RavenConnectionStrings)
                {
                    tasks.Add(_server.SendToLeaderAsync(new PutRavenConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
                }

                result.DatabaseRecord.RavenConnectionStringsUpdated = true;
            }
        }

        if (databaseRecord.SqlConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.SqlConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring SQL connection strings from smuggler");

            foreach (var connectionString in databaseRecord.SqlConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutSqlConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }

            result.DatabaseRecord.SqlConnectionStringsUpdated = true;
        }

        if (databaseRecord.Client != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.Client))
        {
            if (authenticationEnabled && CanAccess(authorizationStatus) == false)
            {
                result.AddError("Import of Client Configuration was skipped due to insufficient permissions on your current certificate.");
                result.DatabaseRecord.ErroredCount++;
            }
            else
            {
                if (_log.IsInfoEnabled)
                    _log.Info("Configuring client configuration from smuggler");

                tasks.Add(_server.SendToLeaderAsync(new EditDatabaseClientConfigurationCommand(databaseRecord.Client, _name, RaftIdGenerator.DontCareId)));
                result.DatabaseRecord.ClientConfigurationUpdated = true;
            }
        }

        if (databaseRecord.Integrations?.PostgreSql != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.PostgreSQLIntegration))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring PostgreSQL integration from smuggler");
            tasks.Add(_server.SendToLeaderAsync(new EditPostgreSqlConfigurationCommand(databaseRecord.Integrations.PostgreSql, _name, RaftIdGenerator.DontCareId)));
            result.DatabaseRecord.PostreSQLConfigurationUpdated = true;
        }

        if (databaseRecord.UnusedDatabaseIds != null && databaseRecord.UnusedDatabaseIds.Count > 0)
        {
            if (_log.IsInfoEnabled)
                _log.Info("Set unused database Ids from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new UpdateUnusedDatabaseIdsCommand(_name, databaseRecord.UnusedDatabaseIds, RaftIdGenerator.DontCareId)));

            result.DatabaseRecord.UnusedDatabaseIdsUpdated = true;
        }

        if (databaseRecordItemType.HasFlag(DatabaseRecordItemType.LockMode))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring database lock mode from smuggler");

            tasks.Add(_server.SendToLeaderAsync(new EditLockModeCommand(_name, databaseRecord.LockMode, RaftIdGenerator.DontCareId)));

            result.DatabaseRecord.LockModeUpdated = true;
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

            result.DatabaseRecord.OlapEtlsUpdated = true;
        }

        if (databaseRecord.OlapConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.OlapConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring OLAP connection strings from smuggler");

            foreach (var connectionString in databaseRecord.OlapConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutOlapConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }

            result.DatabaseRecord.OlapConnectionStringsUpdated = true;
        }

        if (databaseRecord.ElasticSearchConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.ElasticSearchConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring ElasticSearch connection strings from smuggler");
            foreach (var connectionString in databaseRecord.ElasticSearchConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutElasticSearchConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }

            result.DatabaseRecord.ElasticSearchConnectionStringsUpdated = true;
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

            result.DatabaseRecord.ElasticSearchEtlsUpdated = true;
        }

        if (databaseRecord.QueueConnectionStrings.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.QueueConnectionStrings))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring Queue ETL connection strings from smuggler");
            foreach (var connectionString in databaseRecord.QueueConnectionStrings)
            {
                tasks.Add(_server.SendToLeaderAsync(new PutQueueConnectionStringCommand(connectionString.Value, _name, RaftIdGenerator.DontCareId)));
            }

            result.DatabaseRecord.QueueConnectionStringsUpdated = true;
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

            result.DatabaseRecord.QueueEtlsUpdated = true;
        }

        if (databaseRecord.IndexesHistory?.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.IndexesHistory))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring Indexes History configuration from smuggler");

            foreach (var newIndexHistory in databaseRecord.IndexesHistory)
            {
                if (_currentDatabaseRecord.IndexesHistory.ContainsKey(newIndexHistory.Key))
                {
                    tasks.Add(_server.SendToLeaderAsync(new DeleteIndexHistoryCommand(newIndexHistory.Key, _name, RaftIdGenerator.DontCareId)));
                }

                tasks.Add(_server.SendToLeaderAsync(new PutIndexHistoryCommand(newIndexHistory.Key, newIndexHistory.Value, configuration.Indexing.HistoryRevisionsNumber, _name, RaftIdGenerator.DontCareId)));
            }

            result.DatabaseRecord.IndexesHistoryUpdated = true;
        }

        if (databaseRecord.QueueSinks.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.QueueSinks))
        {
            if (_log.IsInfoEnabled)
                _log.Info("Configuring kafka queue sinks configuration from smuggler");
            foreach (var sink in databaseRecord.QueueSinks)
            {
                _currentDatabaseRecord?.QueueSinks.ForEach(x =>
                {
                    if (x.Name.Equals(sink.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        tasks.Add(_server.SendToLeaderAsync(new DeleteOngoingTaskCommand(x.TaskId, OngoingTaskType.QueueSink, _name, RaftIdGenerator.DontCareId)));
                    }
                });
                sink.TaskId = 0;
                sink.Disabled = true;
                tasks.Add(_server.SendToLeaderAsync(new AddQueueSinkCommand(sink, _name, RaftIdGenerator.DontCareId)));
            }

            result.DatabaseRecord.QueueSinksUpdated = true;
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

        using (_server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            List<string> members;

            using (context.OpenReadTransaction())
                members = _server.Cluster.ReadDatabaseTopology(context, _name).Members;

            try
            {
                await _server.WaitForExecutionOnRelevantNodesAsync(context, members, maxIndex);
            }
            catch (RaftIndexWaitAggregateException e)
            {
                throw new InvalidDataException("Respective tasks were dispatched, however, we couldn't achieve consistency across one or more target nodes due to errors.",
                    e);
            }
        }

        tasks.Clear();
    }

    private static bool CanAccess(AuthorizationStatus authorizationStatus)
    {
        return authorizationStatus == AuthorizationStatus.ClusterAdmin ||
               authorizationStatus == AuthorizationStatus.Operator ||
               authorizationStatus == AuthorizationStatus.DatabaseAdmin;
    }

    public ValueTask DisposeAsync()
    {
        return default;
    }
}
