using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
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
            JsonOperationContext jsonOperationContext,
            DatabaseRecord databaseRecord,
            ServerStore server,
            ShardedDatabaseContext databaseContext,
            ShardedDatabaseRequestHandler handler,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result, 
            long operationId,
            Action<IOperationProgress> onProgress = null, 
            CancellationToken token = default) : 
            this(source, new MultiShardedDestination(source, databaseContext, handler, operationId), jsonOperationContext, databaseRecord, server, options, result, onProgress, token)
        {
        }

        internal ShardedDatabaseSmuggler(
            ISmugglerSource source,
            ISmugglerDestination destination,
            JsonOperationContext jsonOperationContext,
            DatabaseRecord databaseRecord,
            ServerStore server,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result,
            Action<IOperationProgress> onProgress = null,
            CancellationToken token = default) :
            base(source, destination, server.Server.Time, jsonOperationContext, options, result, onProgress, token)
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

        protected override async Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result)
        {
            result.Indexes.Start();

            var configuration = DatabasesLandlord.CreateDatabaseConfiguration(_server.Server.ServerStore, _databaseRecord.DatabaseName, _databaseRecord.Settings);

            await foreach (var index in _source.GetIndexesAsync())
            {
                _token.ThrowIfCancellationRequested();
                result.Indexes.ReadCount++;

                if (index == null)
                {
                    result.Indexes.ErroredCount++;
                    continue;
                }

                switch (index.Type)
                {
                    case IndexType.AutoMap:
                        await PutAutoMapIndex(result, index, configuration);
                        break;

                    case IndexType.AutoMapReduce:
                        await PutAutoMapReduceIndex(result, index, configuration);
                        break;

                    case IndexType.Map:
                    case IndexType.MapReduce:
                    case IndexType.JavaScriptMap:
                    case IndexType.JavaScriptMapReduce:
                        await PutIndex(result, index, configuration);
                        break;

                    case IndexType.Faulty:
                        break;

                    default:
                        throw new NotSupportedException(index.Type.ToString());
                }

                if (result.Indexes.ReadCount % 10 == 0)
                {
                    var message = $"Read {result.Indexes.ReadCount:#,#;;0} indexes.";
                    AddInfoToSmugglerResult(result, message);
                }
            }
            return result.Indexes;
        }

        private async Task PutAutoMapReduceIndex(SmugglerResult result, IndexDefinitionAndType index, RavenConfiguration configuration)
        {
            var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)index.IndexDefinition;
            try
            {
                if (IndexStore.IsValidIndexName(autoMapReduceIndexDefinition.Name, false, out var errorMessage) == false)
                {
                    throw new ArgumentException(errorMessage);
                }

                autoMapReduceIndexDefinition.DeploymentMode = configuration.Indexing.AutoIndexDeploymentMode;

                var command = PutAutoIndexCommand.Create(autoMapReduceIndexDefinition, _databaseRecord.DatabaseName, RaftIdGenerator.DontCareId,
                    configuration.Indexing.AutoIndexDeploymentMode);

                await _server.SendToLeaderAsync(command).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                result.Indexes.ErroredCount++;
                result.AddError($"Could not write auto map-reduce index '{autoMapReduceIndexDefinition.Name}': {e.Message}");
            }
        }

        private async Task PutAutoMapIndex(SmugglerResult result, IndexDefinitionAndType index, RavenConfiguration configuration)
        {
            var autoMapIndexDefinition = (AutoMapIndexDefinition)index.IndexDefinition;

            try
            {
                if (IndexStore.IsValidIndexName(autoMapIndexDefinition.Name, false, out var errorMessage) == false)
                {
                    throw new ArgumentException(errorMessage);
                }

                autoMapIndexDefinition.DeploymentMode = configuration.Indexing.AutoIndexDeploymentMode;

                CommandBase command = PutAutoIndexCommand.Create(autoMapIndexDefinition, _databaseRecord.DatabaseName, RaftIdGenerator.DontCareId,
                    configuration.Indexing.AutoIndexDeploymentMode);

                await _server.SendToLeaderAsync(command).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                result.Indexes.ErroredCount++;
                result.AddError($"Could not write auto map index '{autoMapIndexDefinition.Name}': {e.Message}");
            }
        }

        private async Task PutIndex(SmugglerResult result, IndexDefinitionAndType index, RavenConfiguration configuration)
        {
            var indexDefinition = (IndexDefinition)index.IndexDefinition;
            if (string.Equals(indexDefinition.Name, "Raven/DocumentsByEntityName", StringComparison.OrdinalIgnoreCase))
            {
                result.AddInfo("Skipped 'Raven/DocumentsByEntityName' index. It is no longer needed.");
                return;
            }

            if (string.Equals(indexDefinition.Name, "Raven/ConflictDocuments", StringComparison.OrdinalIgnoreCase))
            {
                result.AddInfo("Skipped 'Raven/ConflictDocuments' index. It is no longer needed.");
                return;
            }

            if (indexDefinition.Name.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
            {
                // legacy auto index
                indexDefinition.Name = $"Legacy/{indexDefinition.Name}";
            }

            indexDefinition.DeploymentMode = IndexDeploymentMode.Parallel; //TODO - rolling index
            var command = new PutIndexCommand(
                indexDefinition,
                _databaseRecord.DatabaseName,
                null,
                DateTime.UtcNow,
                RaftIdGenerator.DontCareId,
                configuration.Indexing.HistoryRevisionsNumber,
                configuration.Indexing.StaticIndexDeploymentMode
            );

            try
            {
                await _server.SendToLeaderAsync(command);
            }
            catch (Exception e)
            {
                result.Indexes.ErroredCount++;
                result.AddError($"Could not write map index '{indexDefinition.Name}': {e.Message}");
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessIdentitiesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            await using (var action = new DatabaseKeyValueActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessIdentitiesInternalAsync(result, buildType, action);
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result)
        {
            result.CompareExchange.Start();

            await using (var destinationActions = _destination.CompareExchange(_context))
            await using (var actions = new DatabaseCompareExchangeActions(_server, _databaseRecord, _context, new CancellationToken()))
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
                            await destinationActions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value);
                        }
                        else
                        {
                            await actions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value);
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

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result)
        {
            await using (var destinationActions = _destination.CompareExchangeTombstones(_context))
            await using (var actions = new DatabaseCompareExchangeActions(_server, _databaseRecord, _context, new CancellationToken()))
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
                            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "handle atomic guard tombstones import");
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
            await using(var actions = new SubscriptionActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessSubscriptionsInternalAsync(result, actions);
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result)
        {
            await using (var actions = new ReplicationHubCertificateActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessReplicationHubCertificatesInternalAsync(result, actions);
            }
        }
    }
}
