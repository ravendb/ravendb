using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
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
    public class ShardedDatabaseSmuggler : SmugglerBase
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

        private class ShardedDatabaseCompareExchangeActions : AbstractDatabaseCompareExchangeActions
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
