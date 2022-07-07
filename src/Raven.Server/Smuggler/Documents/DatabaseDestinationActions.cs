using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Integrations.PostgreSQL.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Smuggler.Documents
{
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
                        if ((databaseRecord.ConflictSolverConfig.ResolveByCollection.ContainsKey(collection.Key)) == false)
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
                        if ((databaseRecord.TimeSeries.Collections.ContainsKey(collection.Key)) == false)
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
                        if ((databaseRecord.Revisions.Collections.ContainsKey(collection.Key)) == false)
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

    public class SubscriptionActions : ISubscriptionActions
    {
        private readonly ServerStore _serverStore;
        private readonly string _name;
        private readonly List<PutSubscriptionCommand> _subscriptionCommands = new List<PutSubscriptionCommand>();

        public SubscriptionActions(DocumentDatabase database)
        {
            _serverStore = database.ServerStore;
            /*_name = database is ShardedDocumentDatabase shardedDocumentDatabase
                ? shardedDocumentDatabase.ShardedDatabaseName
                : database.Name;*/
            _name = database.Name;
        }

        public SubscriptionActions(ServerStore serverStore, string name)
        {
            _serverStore = serverStore;
            _name = name;
        }

        public async ValueTask DisposeAsync()
        {
            if (_subscriptionCommands.Count == 0)
                return;

            await SendCommandsAsync();
        }

        public async ValueTask WriteSubscriptionAsync(SubscriptionState subscriptionState)
        {
            const int batchSize = 1024;

            _subscriptionCommands.Add(new PutSubscriptionCommand(_name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
            {
                SubscriptionName = subscriptionState.SubscriptionName,
                //After restore/export , subscription will start from the start
                InitialChangeVector = null
            });

            if (_subscriptionCommands.Count < batchSize)
                return;

            await SendCommandsAsync();
        }

        private async ValueTask SendCommandsAsync()
        {
            await _serverStore.SendToLeaderAsync(new PutSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
            _subscriptionCommands.Clear();
        }
    }

    public class ReplicationHubCertificateActions : IReplicationHubCertificateActions
    {
        private readonly ServerStore _server;
        private readonly string _name;
        private readonly List<RegisterReplicationHubAccessCommand> _commands = new List<RegisterReplicationHubAccessCommand>();

        public ReplicationHubCertificateActions(DocumentDatabase database)
        {
            _server = database.ServerStore;
            _name = database.Name;
        }

        public ReplicationHubCertificateActions(ServerStore serverStore, string name)
        {
            _server = serverStore;
            _name = name;
        }

        public async ValueTask DisposeAsync()
        {
            if (_commands.Count == 0)
                return;

            await SendCommandsAsync();
        }

        public async ValueTask WriteReplicationHubCertificateAsync(string hub, ReplicationHubAccess access)
        {
            const int batchSize = 128;

            byte[] buffer = Convert.FromBase64String(access.CertificateBase64);
            using var cert = new X509Certificate2(buffer);

            _commands.Add(new RegisterReplicationHubAccessCommand(_name, hub, access, cert, RaftIdGenerator.DontCareId));

            if (_commands.Count < batchSize)
                return;

            await SendCommandsAsync();
        }

        private async ValueTask SendCommandsAsync()
        {
            await _server.SendToLeaderAsync(new BulkRegisterReplicationHubAccessCommand
            {
                Commands = _commands,
                Database = _name,
                UniqueRequestId = RaftIdGenerator.DontCareId
            });

            _commands.Clear();
        }
    }

    public class DatabaseKeyValueActions : IKeyValueActions<long>
    {
        private readonly ServerStore _serverStore;
        private readonly string _name;
        private readonly Dictionary<string, long> _identities;

        public DatabaseKeyValueActions(DocumentDatabase database)
        {
            _serverStore = database.ServerStore;
            _name = database.Name;
            _identities = new Dictionary<string, long>();
        }

        public DatabaseKeyValueActions(ServerStore server, string name)
        {
            _serverStore = server;
            _name = name;
            _identities = new Dictionary<string, long>();
        }

        public async ValueTask WriteKeyValueAsync(string key, long value)
        {
            const int batchSize = 1024;

            _identities[key] = value;

            if (_identities.Count < batchSize)
                return;

            await SendIdentitiesAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (_identities.Count == 0)
                return;

            await SendIdentitiesAsync();
        }

        private async ValueTask SendIdentitiesAsync()
        {
            //fire and forget, do not hold-up smuggler operations waiting for Raft command
            await _serverStore.SendToLeaderAsync(new UpdateClusterIdentityCommand(_name, _identities, false, RaftIdGenerator.NewId()));

            _identities.Clear();
        }
    }

    public class DatabaseCompareExchangeActions : ICompareExchangeActions
    {
        const int BatchSize = 1024;

        private readonly ServerStore _serverStore;
        private readonly string _name;
        private readonly JsonOperationContext _context;
        private readonly CancellationToken _token;
        private readonly List<RemoveCompareExchangeCommand> _compareExchangeRemoveCommands = new List<RemoveCompareExchangeCommand>();
        private readonly List<AddOrUpdateCompareExchangeCommand> _compareExchangeAddOrUpdateCommands = new List<AddOrUpdateCompareExchangeCommand>();

        public DatabaseCompareExchangeActions(ServerStore serverStore, DatabaseRecord databaseRecord, JsonOperationContext context, CancellationToken token)
        {
            _serverStore = serverStore;
            _name = databaseRecord.DatabaseName;
            _context = context;
            _token = token;
        }

        public async ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value)
        {
            _compareExchangeAddOrUpdateCommands.Add(new AddOrUpdateCompareExchangeCommand(_name, key, value, 0, _context, RaftIdGenerator.DontCareId,
                fromBackup: true));

            if (_compareExchangeAddOrUpdateCommands.Count >= BatchSize)
                await SendAddOrUpdateCommandsAsync(_context);
        }

        public async ValueTask WriteTombstoneKeyAsync(string key)
        {
            var index = _serverStore.LastRaftCommitIndex;
            _compareExchangeRemoveCommands.Add(new RemoveCompareExchangeCommand(_name, key, index, _context, RaftIdGenerator.DontCareId, fromBackup: true));

            if (_compareExchangeRemoveCommands.Count < BatchSize)
                return;

            await SendRemoveCommandsAsync(_context);
        }

        public async ValueTask DisposeAsync()
        {
            await SendAddOrUpdateCommandsAsync(_context);
            await SendRemoveCommandsAsync(_context);
        }

        private async ValueTask SendAddOrUpdateCommandsAsync(JsonOperationContext context)
        {
            if (_compareExchangeAddOrUpdateCommands.Count == 0)
                return;

            var addOrUpdateResult = await _serverStore.SendToLeaderAsync(new AddOrUpdateCompareExchangeBatchCommand(_compareExchangeAddOrUpdateCommands, context, RaftIdGenerator.DontCareId));
            foreach (var command in _compareExchangeAddOrUpdateCommands)
            {
                command.Value.Dispose();
            }
            _compareExchangeAddOrUpdateCommands.Clear();

            await _serverStore.Cluster.WaitForIndexNotification(addOrUpdateResult.Index);
        }

        private async ValueTask SendRemoveCommandsAsync(JsonOperationContext context)
        {
            if (_compareExchangeRemoveCommands.Count == 0)
                return;
            await _serverStore.SendToLeaderAsync(new AddOrUpdateCompareExchangeBatchCommand(_compareExchangeRemoveCommands, context, RaftIdGenerator.DontCareId));
            _compareExchangeRemoveCommands.Clear();
        }

        public JsonOperationContext GetContextForNewCompareExchangeValue()
        {
            return _context;
        }

        private struct DisposableReturnedArray<T> : IDisposable
        {
            private readonly T[] _array;

            public int Length;

            public DisposableReturnedArray(int length)
            {
                _array = ArrayPool<T>.Shared.Rent(length);
                Length = 0;
            }

            public void Push(T item) => _array[Length++] = item;
            public T this[int index] => _array[index];

            public ArraySegment<T> GetArraySegment() => new ArraySegment<T>(_array, 0, Length);

            public void Clear() => Length = 0;

            public void Dispose() => ArrayPool<T>.Shared.Return(_array);
        }
    }
}
