using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.ShardedHandlers;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
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
    public class ShardedDatabaseSmuggler : SmugglerBase
    {
        private readonly TransactionOperationContext _transactionOperationContext;
        private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocation;
        private readonly DatabaseRecord _databaseRecord;
        private readonly ServerStore _server;
        private int _itemsInBatch;
        private long _buildVersion;
        private readonly ShardedContext _shardedContext;
        private readonly ShardedSmugglerHandler _handler;
        private readonly BlittableJsonReaderObject _optionsAsBlittable;
        private readonly TransactionContextPool _contextPool;

        private readonly List<IAsyncDisposable> _actionsList;
        private readonly List<IAsyncDisposable> _destinationDisposeList;
        private readonly List<ISmugglerDestination> _destinations;
        private readonly List<Stream> _streamList;
        private readonly List<JsonOperationContext> _contextList;

        public ShardedDatabaseSmuggler(TransactionContextPool contextPool,
            ISmugglerSource source,
            JsonOperationContext jsonOperationContext,
            TransactionOperationContext transactionOperationContext,
            DatabaseRecord databaseRecord,
            ServerStore server,
            ShardedContext shardedContext,
            ShardedSmugglerHandler handler,
            BlittableJsonReaderObject optionsAsBlittable,
            DatabaseSmugglerOptionsServerSide options = null,
            SmugglerResult result = null, 
            Action<IOperationProgress> onProgress = null, 
            CancellationToken token = default) : 
            base(source, server.Server.Time, jsonOperationContext, options, result, onProgress, token)
        {
            _contextPool = contextPool;
            _transactionOperationContext = transactionOperationContext;
            _shardAllocation = databaseRecord.ShardAllocations;
            _databaseRecord = databaseRecord;
            _server = server;
            _shardedContext = shardedContext;
            _handler = handler;
            _optionsAsBlittable = optionsAsBlittable;

            _actionsList = new List<IAsyncDisposable>();
            _destinationDisposeList = new List<IAsyncDisposable>();
            _destinations = new List<ISmugglerDestination>();
            _streamList = new List<Stream>();
            _contextList = new List<JsonOperationContext>();
        }

        public override async Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            var result = _result ?? new SmugglerResult();
            using (var initializeResult = await _source.InitializeAsync(_options, result))
            {
                _buildVersion = initializeResult.BuildNumber;

                InitializeDestination(result);

                var buildType = BuildVersion.Type(_buildVersion);
                var currentType = await _source.GetNextTypeAsync();
                while (currentType != DatabaseItemType.None)
                {
                    await ProcessTypeAsync(currentType, result, buildType, ensureStepsProcessed);
                    await CleanActionList();
                    currentType = await _source.GetNextTypeAsync();
                }

                if (ensureStepsProcessed)
                {
                    EnsureProcessed(result);
                }

                await SendImportBatch();

                return result;
            }
        }

        private void InitializeDestination(SmugglerResult result)
        {
            for (int i = 0; i < _shardedContext.ShardCount; i++)
            {
                _streamList.Add(_handler.GetOutputStream(new MemoryStream(), _options));
                _contextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext);
                _contextList.Add(jsonOperationContext);
                _destinations.Add(new StreamDestination(_streamList[i], _contextList[i], _source));
            }
            foreach (var des in _destinations)
            {
                _destinationDisposeList.Add(des.InitializeAsync(_options, result, _buildVersion));
            }
        }

        protected override async Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result)
        {
            await using (var action = new DatabaseRecordActions(_server, _databaseRecord, _databaseRecord.DatabaseName,
                             LoggingSource.Instance.GetLogger<DatabaseDestination>(_databaseRecord.DatabaseName)))
            {
                return await ProcessDatabaseRecordInternalAsync(result, action);
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessDocumentsAsync(SmugglerResult result, BuildVersionType buildType)
        {
            result.Documents.Start();
            SetActionList(DatabaseItemType.Documents);

            await foreach (DocumentItem item in _source.GetDocumentsAsync(_options.Collections))
            {
                await HandleImportBatch(DatabaseItemType.Documents, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, item.Document.Id);
                await ((IDocumentActions)_actionsList[index]).WriteDocumentAsync(item, result.Documents);
                DisposeSourceInfo();
                _itemsInBatch++;
            }

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessRevisionDocumentsAsync(SmugglerResult result)
        {
            result.RevisionDocuments.Start();
            SetActionList(DatabaseItemType.RevisionDocuments);
            await foreach (var item in _source.GetRevisionDocumentsAsync(_options.Collections))
            {
                await HandleImportBatch(DatabaseItemType.RevisionDocuments, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, item.Document.Id);
                await ((IDocumentActions)_actionsList[index]).WriteDocumentAsync(item, result.RevisionDocuments);
                DisposeSourceInfo();
                _itemsInBatch++;
                result.RevisionDocuments.LastEtag = item.Document.Etag;
            }

            return result.RevisionDocuments;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTombstonesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            SetActionList(DatabaseItemType.Tombstones);
            await foreach (var tombstone in _source.GetTombstonesAsync(_options.Collections))
            {
                await HandleImportBatch(DatabaseItemType.Tombstones, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, tombstone.LowerId);
                await ((IDocumentActions)_actionsList[index]).WriteTombstoneAsync(tombstone, result.Tombstones);
                _itemsInBatch++;
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
            SetActionList(DatabaseItemType.Conflicts);
            await foreach (var conflict in _source.GetConflictsAsync(_options.Collections))
            {
                await HandleImportBatch(DatabaseItemType.Conflicts, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, conflict.Id);
                result.Conflicts.LastEtag = conflict.Etag;
                await ((IDocumentActions)_actionsList[index]).WriteConflictAsync(conflict, result.Conflicts);
                _itemsInBatch++;
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
            await using (var action = new DatabaseKeyValueActions(_server, _databaseRecord.DatabaseName))
            {
                return await ProcessIdentitiesInternalAsync(result, buildType, action);
            }
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentsAsync(SmugglerResult result)
        {
            SetActionList(DatabaseItemType.LegacyAttachments);
            await foreach (DocumentItem item in _source.GetLegacyAttachmentsAsync(null))
            {
                await HandleImportBatch(DatabaseItemType.LegacyAttachments, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, item.Document.Id);

                if (item.Document.Id == null)
                    ThrowInvalidData();

                item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                await ((IDocumentActions)_actionsList[index]).WriteDocumentAsync(item, result.Documents);
                DisposeSourceInfo();
                _itemsInBatch++;
            }

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyDocumentDeletionsAsync(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            SetActionList(DatabaseItemType.LegacyDocumentDeletions);
            await foreach (var id in _source.GetLegacyDocumentDeletionsAsync())
            {
                await HandleImportBatch(DatabaseItemType.LegacyDocumentDeletions, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, id);
                await ((ILegacyActions)_actionsList[index]).WriteLegacyDeletions(id);
                _itemsInBatch++;
            }

            return counts;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentDeletionsAsync(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            SetActionList(DatabaseItemType.LegacyAttachmentDeletions);
            await foreach (var id in _source.GetLegacyAttachmentDeletionsAsync())
            {
                await HandleImportBatch(DatabaseItemType.LegacyAttachmentDeletions, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, id);
                await ((ILegacyActions)_actionsList[index]).WriteLegacyDeletions(id);
                _itemsInBatch++;
            }

            return counts;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result)
        {
            result.CompareExchange.Start();

            SetActionList(DatabaseItemType.CompareExchange);
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
                        if (ClusterTransactionCommand.IsAtomicGuardKey(kvp.Key.Key, out var docId))
                        {
                            await HandleImportBatch(DatabaseItemType.CompareExchange, result);
                            var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, docId);
                            await ((ICompareExchangeActions)_actionsList[index]).WriteKeyValueAsync(kvp.Key.Key, kvp.Value);
                            _itemsInBatch++;
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

        protected override async Task<SmugglerProgressBase.Counts> ProcessCountersAsync(SmugglerResult result)
        {
            result.Counters.Start();
            SetActionList(DatabaseItemType.CounterGroups);
            var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
            await foreach (var counterGroup in _source.GetCounterValuesAsync(_options.Collections, null))
            {
                await HandleImportBatch(DatabaseItemType.CounterGroups, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, counterGroup.DocumentId);
                await ((ICounterActions)_actionsList[index]).WriteCounterAsync(counterGroup);
                DisposeSourceInfo();
                _itemsInBatch++;
            }
            return result.Counters;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyCountersAsync(SmugglerResult result)
        {
            result.Counters.Start();
            SetActionList(DatabaseItemType.Counters);
            var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
            await foreach (var counter in _source.GetLegacyCounterValuesAsync())
            {
                await HandleImportBatch(DatabaseItemType.Counters, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, counter.DocumentId);
                await ((ICounterActions)_actionsList[index]).WriteLegacyCounterAsync(counter);
                _itemsInBatch++;
            }
            return result.Counters;
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

        protected override async Task<SmugglerProgressBase.Counts> ProcessTimeSeriesAsync(SmugglerResult result)
        {
            result.TimeSeries.Start();
            SetActionList(DatabaseItemType.TimeSeries);
            var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
            await foreach (var ts in _source.GetTimeSeriesAsync(_options.Collections))
            {
                await HandleImportBatch(DatabaseItemType.TimeSeries, result);
                var index = ShardHelper.GetShardIndexforDocument(_transactionOperationContext, _shardAllocation, ts.DocId);
                await ((ITimeSeriesActions)_actionsList[index]).WriteTimeSeriesAsync(ts);
                _itemsInBatch++;
            }
            return result.TimeSeries;
        }

        private async Task HandleImportBatch(DatabaseItemType type, SmugglerResult result)
        {
            if (_itemsInBatch == 1000)
            {
                await CleanActionList();
                await SendImportBatch();

                for (int i = 0; i < _shardedContext.ShardCount; i++)
                {
                    _contextList[i].Dispose();
                    await _streamList[i].DisposeAsync();

                }
                _itemsInBatch = 0;
                _destinations.Clear();
                _contextList.Clear();
                _streamList.Clear();
                _destinationDisposeList.Clear();
                InitializeDestination(result);
                SetActionList(type);
            }
        }

        private void SetActionList(DatabaseItemType type)
        {
            foreach (var des in _destinations)
            {
                switch (type)
                {
                    case DatabaseItemType.Documents:
                        _actionsList.Add(des.Documents(_options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false));
                        break;
                    case DatabaseItemType.RevisionDocuments:
                        _actionsList.Add(des.RevisionDocuments());
                        break;
                    case DatabaseItemType.Tombstones:
                        _actionsList.Add(des.Tombstones());
                        break;
                    case DatabaseItemType.Conflicts:
                        _actionsList.Add(des.Conflicts());
                        break;
                    case DatabaseItemType.LegacyAttachments:
                        _actionsList.Add(des.Documents());
                        break;
                    case DatabaseItemType.LegacyDocumentDeletions:
                        _actionsList.Add(des.LegacyDocumentDeletions());
                        break;
                    case DatabaseItemType.LegacyAttachmentDeletions:
                        _actionsList.Add(des.LegacyAttachmentDeletions());
                        break;
                    case DatabaseItemType.Counters:
                        _actionsList.Add(des.LegacyCounters(_result));
                        break;
                    case DatabaseItemType.CounterGroups:
                        _actionsList.Add(des.Counters(_result));
                        break;
                    case DatabaseItemType.TimeSeries:
                        _actionsList.Add(des.TimeSeries());
                        break;
                    case DatabaseItemType.CompareExchange:
                        _actionsList.Add(des.CompareExchange(_context));
                        break;
                }
            }
        }

        public async Task SendImportBatch()
        {
            for (int i = 0; i < _shardedContext.ShardCount; i++)
            {
                await _destinationDisposeList[i].DisposeAsync();

            }
            var tasks = new List<Task>();
            for (int i = 0; i < _shardedContext.ShardCount; i++)
            {
                _streamList[i].Position = 0;

                var rel = _contextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext);

                var multi = new MultipartFormDataContent
                {
                    {
                        new BlittableJsonContent(async stream2 => await jsonOperationContext.WriteAsync(stream2, _optionsAsBlittable).ConfigureAwait(false)),
                        Constants.Smuggler.ImportOptions
                    },
                    {new Client.Documents.Smuggler.DatabaseSmuggler.StreamContentWithConfirmation(_streamList[i], new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously)), "file", "name"}
                };
                var cmd = new ShardedImportCommand(_handler, Headers.None, multi);

                var req = _shardedContext.RequestExecutors[i].ExecuteAsync(cmd, jsonOperationContext);
                req.ContinueWith(_ => rel.Dispose());
                tasks.Add(req);
            }
            await tasks.WhenAll();
        }

        private void DisposeSourceInfo()
        {
            if (_source is StreamSource streamSource)
            {
                foreach (var iDisposable in streamSource.ToDispose)
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
