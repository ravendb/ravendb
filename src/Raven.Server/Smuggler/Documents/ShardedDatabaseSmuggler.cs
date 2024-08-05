using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Actions;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Smuggler.Documents
{
    public sealed class ShardedDatabaseSmuggler : SmugglerBase
    {
        private readonly DatabaseRecord _databaseRecord;
        private readonly ServerStore _server;

        public ShardedDatabaseSmuggler(
            ISmugglerSource source,
            ISmugglerDestination destination,
            JsonOperationContext jsonOperationContext,
            DatabaseRecord databaseRecord,
            ServerStore server,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result,
            Action<IOperationProgress> onProgress = null,
            CancellationToken token = default) :
            base(databaseRecord.DatabaseName, source, destination, server.Server.Time, jsonOperationContext, options, result, onProgress, token)
        {
            _databaseRecord = databaseRecord;
            _server = server;
        }

        public override SmugglerPatcher CreatePatcher() => new ServerSmugglerPatcher(_options, _server);

        protected override async Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result)
        {
            await using (var action = new DatabaseRecordActions(_server, _databaseRecord, _databaseRecord.DatabaseName,
                             LoggingSource.Instance.GetLogger<DatabaseDestination>(_databaseRecord.DatabaseName)))
            {
                return await ProcessDatabaseRecordInternalAsync(result, action);
            }
        }

        protected override void SkipDatabaseRecordTypesIfNeeded(DatabaseRecord databaseRecord, SmugglerResult result, DatabaseRecordItemType databaseRecordItemType)
        {
            if (databaseRecord.SinkPullReplications.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.SinkPullReplications))
            {
                foreach (var pullReplication in databaseRecord.SinkPullReplications)
                {
                    AddWarning(DatabaseRecordItemType.SinkPullReplications, pullReplication.Name);
                }
            }

            if (databaseRecord.HubPullReplications.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.HubPullReplications))
            {
                foreach (var pullReplication in databaseRecord.HubPullReplications)
                {
                    AddWarning(DatabaseRecordItemType.HubPullReplications, pullReplication.Name);
                }
            }

            if (databaseRecord.Integrations?.PostgreSql != null && databaseRecordItemType.HasFlag(DatabaseRecordItemType.PostgreSQLIntegration))
            {
                AddWarning(DatabaseRecordItemType.PostgreSQLIntegration);
            }

            if (databaseRecord.QueueEtls.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.QueueEtls))
            {
                AddWarning(DatabaseRecordItemType.QueueEtls);
            }

            if (databaseRecord.QueueSinks.Count > 0 && databaseRecordItemType.HasFlag(DatabaseRecordItemType.QueueSinks))
            {
                AddWarning(DatabaseRecordItemType.QueueSinks);
            }

            //warn and remove mentor nodes
            foreach (var externalReplication in databaseRecord.ExternalReplications)
            {
                if (string.IsNullOrEmpty(externalReplication.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.ExternalReplications, externalReplication.Name);
                    externalReplication.MentorNode = null;
                }
            }
            
            foreach (var queueEtl in databaseRecord.QueueEtls)
            {
                if (string.IsNullOrEmpty(queueEtl.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.QueueEtls, queueEtl.Name);
                    queueEtl.MentorNode = null;
                }
            }

            foreach (var ravenEtl in databaseRecord.RavenEtls)
            {
                if (string.IsNullOrEmpty(ravenEtl.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.RavenEtls, ravenEtl.Name);
                    ravenEtl.MentorNode = null;
                }
            }

            foreach (var elasticEtl in databaseRecord.ElasticSearchEtls)
            {
                if (string.IsNullOrEmpty(elasticEtl.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.ElasticSearchEtls, elasticEtl.Name);
                    elasticEtl.MentorNode = null;
                }
            }

            foreach (var olapEtl in databaseRecord.OlapEtls)
            {
                if (string.IsNullOrEmpty(olapEtl.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.OlapEtls, olapEtl.Name);
                    olapEtl.MentorNode = null;
                }
            }

            foreach (var sqlEtl in databaseRecord.SqlEtls)
            {
                if (string.IsNullOrEmpty(sqlEtl.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.SqlEtls, sqlEtl.Name);
                    sqlEtl.MentorNode = null;
                }
            }

            foreach (var backup in databaseRecord.PeriodicBackups)
            {
                if (string.IsNullOrEmpty(backup.MentorNode) == false)
                {
                    AddMentorNodeWarning(DatabaseRecordItemType.PeriodicBackups, backup.Name);
                    backup.MentorNode = null;
                }
            }

            _options.OperateOnDatabaseRecordTypes &= ~DatabaseSmugglerOptions.ShardingNotSupportedDatabaseSmugglerOptions;

            void AddWarning(DatabaseRecordItemType type, string name = null)
            {
                var typeMsg = string.IsNullOrEmpty(name) ? $"{type}" : $"{type} task '{name}'";
                result.AddWarning($"Skipped {typeMsg} - it is currently not supported in Sharding");
            }

            void AddMentorNodeWarning(DatabaseRecordItemType type, string name = null)
            {
                var typeMsg = string.IsNullOrEmpty(name) ? $"{type}" : $"{type} task '{name}'";
                result.AddWarning($"Removed mentor node for {typeMsg} - it is currently not supported in Sharding");
            }
        }

        protected override bool ShouldSkipIndex(IndexDefinitionAndType index, out string msg)
        {
            msg = null;

            if (index.Type == IndexType.AutoMap || index.Type == IndexType.AutoMapReduce)
                return false;

            var definition = (IndexDefinition)index.IndexDefinition;

            if (definition.OutputReduceToCollection != null)
            {
                msg = $"Skipped index '{definition.Name}'. Map-Reduce output documents feature is currently not supported in Sharding";
                return true;
            }

            if (definition.DeploymentMode is IndexDeploymentMode.Rolling)
            {
                msg = $"Skipped index '{definition.Name}'. Rolling indexes are currently not supported in Sharding";
                return true;
            }

            return false;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessIdentitiesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            await using (var action = new DatabaseKeyValueActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessIdentitiesInternalAsync(result, buildType, action);
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result, string databaseName)
        {
            result.CompareExchange.Start();

            await using (var destinationActions = _destination.CompareExchange(databaseName, _context, BackupKind, withDocuments: false))
            await using (var actions = new ShardedDatabaseCompareExchangeActions(_server, _databaseRecord, _context, BackupKind, _token))
            {
                await foreach (var kvp in _source.GetCompareExchangeValuesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.CompareExchange.ReadCount++;
                    if (result.CompareExchange.ReadCount != 0 && result.CompareExchange.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.CompareExchange.ReadCount:#,#;;0} compare exchange values.");

                    if (kvp.Equals(default))
                    {
                        result.CompareExchange.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        if (ClusterTransactionCommand.IsAtomicGuardKey(kvp.Key.Key, out _))
                        {
                            await destinationActions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value, existingDocument: null);
                        }
                        else
                        {
                            await actions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value, existingDocument: null);
                        }

                        result.CompareExchange.LastEtag = kvp.Index;
                    }
                    catch (Exception e)
                    {
                        result.CompareExchange.ErroredCount++;
                        result.AddError($"Could not write compare exchange '{kvp.Key.Key}->{kvp.Value}': {e.Message}");
                    }
                }

            }

            return result.CompareExchange;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result, string databaseName)
        {
            await using (var destinationActions = _destination.CompareExchangeTombstones(databaseName, _context))
            await using (var actions = new ShardedDatabaseCompareExchangeActions(_server, _databaseRecord, _context, BackupKind, _token))
            {
                await foreach (var kvp in _source.GetCompareExchangeTombstonesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.CompareExchangeTombstones.ReadCount++;

                    if (kvp.Equals(default))
                    {
                        result.CompareExchangeTombstones.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        if (ClusterTransactionCommand.IsAtomicGuardKey(kvp.Key.Key, out _))
                        {
                            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal,
                                "RavenDB-19201 : handle atomic guard tombstones import");
                            await destinationActions.WriteTombstoneKeyAsync(kvp.Key.Key);
                        }
                        else
                        {
                            await actions.WriteTombstoneKeyAsync(kvp.Key.Key);
                        }
                    }
                    catch (Exception e)
                    {
                        result.CompareExchangeTombstones.ErroredCount++;
                        result.AddError($"Could not write compare exchange tombstone '{kvp.Key.Key}': {e.Message}");
                    }
                }
            }

            return result.CompareExchangeTombstones;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessSubscriptionsAsync(SmugglerResult result)
        {
            await using (var actions = new ShardedDatabaseSubscriptionActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessSubscriptionsInternalAsync(result, actions);
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result)
        {
            await using (var actions = new DatabaseReplicationHubCertificateActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessReplicationHubCertificatesInternalAsync(result, actions);
            }
        }

        private sealed class ShardedDatabaseCompareExchangeActions : AbstractDatabaseCompareExchangeActions
        {
            public ShardedDatabaseCompareExchangeActions(ServerStore serverStore, DatabaseRecord databaseRecord, JsonOperationContext context, BackupKind? backupKind, CancellationToken token) 
                : base(serverStore, databaseRecord.DatabaseName, databaseRecord.Client?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator, context, backupKind, token)
            {
            }

            protected override bool TryHandleAtomicGuard(string key, string documentId, BlittableJsonReaderObject value, Document existingDocument)
            {
                return false;
            }

            protected override async ValueTask WaitForIndexNotificationAsync(long? lastAddOrUpdateOrRemoveResultIndex, long? lastClusterTransactionIndex)
            {
                var index = Math.Max(lastAddOrUpdateOrRemoveResultIndex ?? 0, lastClusterTransactionIndex ?? 0);
                if (index == 0)
                    return;

                await _serverStore.Cluster.WaitForIndexNotification(index);
            }
        }
    }
}
