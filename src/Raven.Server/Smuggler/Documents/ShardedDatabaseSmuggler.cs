using System;
using System.Collections.Generic;
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
using Raven.Server.Documents.ShardedHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Smuggler.Documents
{
    public class ShardedDatabaseSmuggler : BaseSmuggler 
    {
        private readonly TransactionOperationContext _transactionOperationContext;
        private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocation;
        private readonly List<ISmugglerDestination> _destinations;
        private readonly DatabaseRecord _databaseRecord;
        private readonly ServerStore _server;
        private int _itemsInBatch;
        private readonly List<IAsyncDisposable> _actionsList;
        private readonly List<IAsyncDisposable> _desList;
        private long _buildVersion;
        public ShardedSmugglerHandler.ImportBatch ImportBatch;

        public ShardedDatabaseSmuggler( ISmugglerSource source, 
            List<ISmugglerDestination> destination, 
            JsonOperationContext context, 
            TransactionOperationContext transactionOperationContext,
            DatabaseRecord databaseRecord,
            ServerStore server,
            SystemTime time, DatabaseSmugglerOptionsServerSide options = null,
            SmugglerResult result = null, 
            Action<IOperationProgress> onProgress = null, 
            CancellationToken token = default) : 
            base(source, time, context, options, result, onProgress, token)
        {
            _transactionOperationContext = transactionOperationContext;
            _shardAllocation = databaseRecord.ShardAllocations;
            _databaseRecord = databaseRecord;
            _destinations = destination;
            _server = server;
            _actionsList = new List<IAsyncDisposable>();
            _desList = new List<IAsyncDisposable>();
        }

        //TODO - handle result
        public override async Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            var result = _result ?? new SmugglerResult();
            try
            {
                using (var initializeResult = await _source.InitializeAsync(_options, result))
                {
                    _buildVersion = initializeResult.BuildNumber;
                    foreach (var des in _destinations)
                    {
                        _desList.Add(des.InitializeAsync(_options, result, _buildVersion));
                    }

                    var buildType = BuildVersion.Type(_buildVersion);
                    var currentType = await _source.GetNextTypeAsync();
                    while (currentType != DatabaseItemType.None)
                    {
                        await ProcessTypeAsync(currentType, result, buildType, ensureStepsProcessed);

                        currentType = await _source.GetNextTypeAsync();
                    }

                    if (ensureStepsProcessed)
                    {
                        EnsureProcessed(result);
                    }

                    return result;
                }
            }
            finally
            {
                foreach (var des in _desList)
                {
                    await des.DisposeAsync();
                }
            }
        }
        
        protected override async Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result)
        {
            var action = new DatabaseRecordActions(_server, _databaseRecord, _databaseRecord.DatabaseName, LoggingSource.Instance.GetLogger<DatabaseDestination>(_databaseRecord.DatabaseName));

            var databaseRecord = await _source.GetDatabaseRecordAsync();
            _token.ThrowIfCancellationRequested();
            //TODO - OnDatabaseRecordAction
            result.DatabaseRecord.ReadCount++;
            try
            {
                await action.WriteDatabaseRecordAsync(databaseRecord, result.DatabaseRecord, _options.AuthorizationStatus, _options.OperateOnDatabaseRecordTypes);
            }
            catch (Exception e)
            {
                result.AddError($"Could not write database record: {e.Message}");
                result.DatabaseRecord.ErroredCount++;
                throw;
            }
            return result.DatabaseRecord;
        }

        private async void HandleImportBatch(string type)
        {
            if (_itemsInBatch == 1000)
            {
                await SendBatch();

                foreach (var des in _destinations)
                {
                    _desList.Add(des.InitializeAsync(_options, _result, _buildVersion));

                    switch (type)
                    {
                        case "Docs":
                            _actionsList.Add(des.Documents(_options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false));
                            break;
                        case "Revisions":
                            _actionsList.Add(des.RevisionDocuments());
                            break;
                        case "Tombstones":
                            _actionsList.Add(des.Tombstones());
                            break;
                        case "Conflicts":
                            _actionsList.Add(des.Conflicts());
                            break;
                        case "LegacyAttachments":
                            _actionsList.Add(des.Documents());
                            break;
                        case "LegacyDocumentDeletions":
                            _actionsList.Add(des.LegacyDocumentDeletions());
                            break;
                        case "LegacyAttachmentDeletions":
                            _actionsList.Add(des.LegacyAttachmentDeletions());
                            break;
                        case "LegacyCounters":
                            _actionsList.Add(des.LegacyCounters(_result));
                            break;
                        case "Counters":
                            _actionsList.Add(des.Counters(_result));
                            break;
                        case "TimeSeries":
                            _actionsList.Add(des.TimeSeries());
                            break;
                    }
                }
            }
        }

        private async Task SendBatch()
        {
            await CleanActionList();
            foreach (var des in _desList)
            {
                await des.DisposeAsync();
            }

            var task = ImportBatch.SendImportBatch();
            task.Wait();
            _itemsInBatch = 0;
            _desList.Clear();
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessDocumentsAsync(SmugglerResult result, BuildVersionType buildType)
        {
            var throwOnCollectionMismatchError = _options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false;
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.Documents(throwOnCollectionMismatchError));
                }
                await foreach (DocumentItem item in _source.GetDocumentsAsync(_options.Collections))
                {
                    HandleImportBatch("Docs");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, item.Document.Id);
                    await ((IDocumentActions)_actionsList[index]).WriteDocumentAsync(item, result.Documents);
                    DisposeSourceInfo();
                    _itemsInBatch++;
                }
            }
            finally
            {
                await CleanActionList();
            }

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessRevisionDocumentsAsync(SmugglerResult result)
        {
            result.RevisionDocuments.Start();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.RevisionDocuments());
                }
            
                await foreach (var item in _source.GetRevisionDocumentsAsync(_options.Collections))
                {
                    HandleImportBatch("Revisions");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, item.Document.Id);
                    await ((IDocumentActions)_actionsList[index]).WriteDocumentAsync(item, result.RevisionDocuments);
                    DisposeSourceInfo();
                    _itemsInBatch++;
                    result.RevisionDocuments.LastEtag = item.Document.Etag;
                }

                return result.RevisionDocuments;
            }
            finally
            {
                await CleanActionList();
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTombstonesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            await CleanActionList();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.Tombstones());
                }

                await foreach (var tombstone in _source.GetTombstonesAsync(_options.Collections))
                {
                    HandleImportBatch("Tombstones");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, tombstone.LowerId);
                    await ((IDocumentActions)_actionsList[index]).WriteTombstoneAsync(tombstone, result.Tombstones);
                    _itemsInBatch++;
                }
            }
            finally
            {
                await CleanActionList();

            }


            return result.Documents;
        }

        protected override async Task ProcessDocumentsWithDuplicateCollectionAsync(SmugglerResult result)
        {
            return;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessConflictsAsync(SmugglerResult result)
        {
            result.Conflicts.Start();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.Conflicts());
                }

                await foreach (var conflict in _source.GetConflictsAsync(_options.Collections))
                {
                    HandleImportBatch("Conflicts");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, conflict.Id);
                    result.Conflicts.LastEtag = conflict.Etag;
                    await ((IDocumentActions)_actionsList[index]).WriteConflictAsync(conflict, result.Conflicts);
                    _itemsInBatch++;
                }
            }
            finally
            {
                await CleanActionList();
            }

            return result.Conflicts;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result)
        {
            result.Indexes.Start();

            var configuration = _server.DatabasesLandlord.CreateConfiguration(_databaseRecord);
            
            
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
            result.Identities.Start();

            await using (var actions = new DatabaseKeyValueActions(_server, _databaseRecord.DatabaseName))
            {
                await foreach (var identity in _source.GetIdentitiesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Identities.ReadCount++;

                    if (identity.Equals(default))
                    {
                        result.Identities.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        string identityPrefix = identity.Prefix;
                        if (buildType == BuildVersionType.V3)
                        {
                            // ends with a "/"
                            identityPrefix = identityPrefix.Substring(0, identityPrefix.Length - 1) + "|";
                        }

                        await actions.WriteKeyValueAsync(identityPrefix, identity.Value);
                        result.Identities.LastEtag = identity.Index;
                    }
                    catch (Exception e)
                    {
                        result.Identities.ErroredCount++;
                        result.AddError($"Could not write identity '{identity.Prefix}->{identity.Value}': {e.Message}");
                    }
                }
            }
            return result.Identities;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentsAsync(SmugglerResult result)
        {
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.Documents());
                }

                await foreach (DocumentItem item in _source.GetLegacyAttachmentsAsync(null))
                {
                    HandleImportBatch("LegacyAttachments");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, item.Document.Id);

                    if (item.Document.Id == null)
                        ThrowInvalidData();

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;
                    
                    await ((IDocumentActions)_actionsList[index]).WriteDocumentAsync(item, result.Documents);
                    DisposeSourceInfo();
                    _itemsInBatch++;
                }
            }
            finally
            {
                await CleanActionList();
            }

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyDocumentDeletionsAsync(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.LegacyDocumentDeletions());
                }

                await foreach (var id in _source.GetLegacyDocumentDeletionsAsync())
                {
                    HandleImportBatch("LegacyDocumentDeletions");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, id);
                    await ((ILegacyActions)_actionsList[index]).WriteLegacyDeletions(id);
                    _itemsInBatch++;
                }

                return counts;
            }
            finally
            {
                await CleanActionList();
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentDeletionsAsync(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.LegacyAttachmentDeletions());
                }

                await foreach (var id in _source.GetLegacyAttachmentDeletionsAsync())
                {
                    HandleImportBatch("LegacyAttachmentDeletions");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, id);
                    await ((ILegacyActions)_actionsList[index]).WriteLegacyDeletions(id);
                    _itemsInBatch++;
                }

                return counts;
            }
            finally
            {
                await CleanActionList();
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result)
        {
            result.CompareExchange.Start();
            
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
                        await actions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value);
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

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyCountersAsync(SmugglerResult result)
        {
            result.Counters.Start();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.LegacyCounters(result));
                }
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var counter in _source.GetLegacyCounterValuesAsync())
                {
                    HandleImportBatch("LegacyCounters");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, counter.DocumentId);
                    await ((ICounterActions)_actionsList[index]).WriteLegacyCounterAsync(counter);
                    _itemsInBatch++;
                }
                return result.Counters;
            }
            finally
            {
                await CleanActionList();
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCountersAsync(SmugglerResult result)
        {
            result.Counters.Start();
            try
            {
                foreach (var des in _destinations)
                {
                    _actionsList.Add(des.Counters(result));
                }
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var counterGroup in _source.GetCounterValuesAsync(_options.Collections, null))
                {
                    HandleImportBatch("Counters");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, counterGroup.DocumentId);
                    await ((ICounterActions)_actionsList[index]).WriteCounterAsync(counterGroup);
                    DisposeSourceInfo();
                    _itemsInBatch++;
                }
                return result.Counters;
            }
            finally
            {
                await CleanActionList();
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result)
        {
            var actionsList = new List<ICompareExchangeActions>();

            try
            {
                foreach (var des in _destinations)
                {
                    actionsList.Add(des.CompareExchangeTombstones(_transactionOperationContext));
                }

                await foreach (var kvp in _source.GetCompareExchangeTombstonesAsync())
                {
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, kvp.Key.Key);


                    await actionsList[index].WriteTombstoneKeyAsync(kvp.Key.Key);
                }

                return result.CompareExchangeTombstones;
            }
            finally
            {
                foreach (var action in actionsList)
                {
                    await action.DisposeAsync();
                }
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessSubscriptionsAsync(SmugglerResult result)
        {
            result.Subscriptions.Start();

            await using(var actions = new SubscriptionActions(_server, _databaseRecord.DatabaseName))
            {
                await foreach (var subscription in _source.GetSubscriptionsAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Subscriptions.ReadCount++;

                    if (result.Subscriptions.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Subscriptions.ReadCount:#,#;;0} subscription.");

                    await actions.WriteSubscriptionAsync(subscription);
                }

                return result.Subscriptions;
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result)
        {
            result.ReplicationHubCertificates.Start();

            await using (var actions = new ReplicationHubCertificateActions(_server, _databaseRecord.DatabaseName))
            {
                await foreach (var (hub, access) in _source.GetReplicationHubCertificatesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.ReplicationHubCertificates.ReadCount++;

                    if (result.ReplicationHubCertificates.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.ReplicationHubCertificates.ReadCount:#,#;;0} subscription.");

                    await actions.WriteReplicationHubCertificateAsync(hub, access);
                }
            }

            return result.ReplicationHubCertificates;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTimeSeriesAsync(SmugglerResult result)
        {
            result.TimeSeries.Start();
            try
            {
                for (int i = 0; i < _destinations.Count; i++)
                {
                    _actionsList.Add(_destinations[i].TimeSeries());
                }
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var ts in _source.GetTimeSeriesAsync(_options.Collections))
                {
                    HandleImportBatch("TimeSeries");
                    var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, ts.DocId);
                    await ((ITimeSeriesActions)_actionsList[index]).WriteTimeSeriesAsync(ts);
                    _itemsInBatch++;
                }
                return result.TimeSeries;
            }
            finally
            {
                await CleanActionList();
            }
        }

        private void DisposeSourceInfo()
        {
            if (_source is StreamSource streamSource)
            {
                foreach (var iDisposable in streamSource._toDispose)
                {
                    iDisposable.Dispose();
                }
            }
        }

        private async Task CleanActionList()
        {
            foreach (var action in _actionsList)
            {
                await action.DisposeAsync();
            }
            _actionsList.Clear();
        }
    }
}
