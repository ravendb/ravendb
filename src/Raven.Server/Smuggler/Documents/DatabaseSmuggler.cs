using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSmuggler : BaseSmuggler
    {
        private readonly ISmugglerDestination _destination;
        private readonly SmugglerPatcher _patcher;
        private readonly TransactionContextPool _transactionContextPool;
        public Action<IndexDefinitionAndType> OnIndexAction;

        public const string PreV4RevisionsDocumentId = "/revisions/";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPreV4Revision(BuildVersionType buildType, string id, Document document)
        {
            if (buildType == BuildVersionType.V3 == false)
                return false;

            if ((document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) != NonPersistentDocumentFlags.LegacyRevision)
                return false;

            return id.Contains(PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase);
        }

        public DatabaseSmuggler(DocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time, JsonOperationContext context,
            DatabaseSmugglerOptionsServerSide options = null, SmugglerResult result = null, Action<IOperationProgress> onProgress = null,
            CancellationToken token = default) : base(source, time, context, options, result, onProgress, token )
        {
            _destination = destination;
            _transactionContextPool = database.ServerStore.ContextPool;
            if (string.IsNullOrWhiteSpace(_options.TransformScript) == false)
                _patcher = new SmugglerPatcher(_options, database);

            Debug.Assert((source is DatabaseSource && destination is DatabaseDestination) == false,
                "When both source and destination are database, we might get into a delayed write for the dest while the " +
                "source already pulsed its' read transaction, resulting in bad memory read.");

        }

        /// <summary>
        /// isLastFile param true by default to correctly restore identities and compare exchange from V41 ravendbdump file.
        /// </summary>
        /// <param name="ensureStepsProcessed"></param>
        /// <param name="isLastFile"></param>
        /// <returns></returns>
        public override async Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            var result = _result ?? new SmugglerResult();
            var sharded = false;
            if (_source is DatabaseSource)
            {
                var record = await _source.GetDatabaseRecordAsync();

                if (ShardHelper.IsShardedName(record.DatabaseName))
                {
                    sharded = true;
                }
            }

            using (_patcher?.Initialize())
            using (var initializeResult = await _source.InitializeAsync(_options, result))
            await using (_destination.InitializeAsync(_options, result, initializeResult.BuildNumber, sharded))
            {
                ModifyV41OperateOnTypes(initializeResult.BuildNumber, isLastFile);

                var buildType = BuildVersion.Type(initializeResult.BuildNumber);
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

        protected override async Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result)
        {
            return await ProcessDatabaseRecordInternalAsync(result, _destination.DatabaseRecord());
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessDocumentsAsync(SmugglerResult result, BuildVersionType buildType)
        {
            result.Documents.Start();

            var throwOnCollectionMismatchError = _options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false;

            await using (var actions = _destination.Documents(throwOnCollectionMismatchError))
            {
                List<LazyStringValue> legacyIdsToDelete = null;

                await foreach (DocumentItem item in _source.GetDocumentsAsync(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();

                    var isPreV4Revision = IsPreV4Revision(buildType, item.Document.Id, item.Document);
                    if (isPreV4Revision)
                    {
                        result.RevisionDocuments.ReadCount++;
                    }
                    else
                    {
                        result.Documents.ReadCount++;
                    }

                    if (result.Documents.ReadCount % 1000 == 0)
                    {
                        var message = $"Read {result.Documents.ReadCount:#,#;;0} documents.";
                        if (result.Documents.Attachments.ReadCount > 0)
                            message += $" Read {result.Documents.Attachments.ReadCount:#,#;;0} attachments.";
                        AddInfoToSmugglerResult(result, message);
                    }

                    if (item.Document == null)
                    {
                        result.Documents.ErroredCount++;
                        if (result.Documents.ErroredCount % 1000 == 0)
                            AddInfoToSmugglerResult(result, $"Error Count: {result.Documents.ErroredCount:#,#;;0}.");
                        continue;
                    }

                    if (item.Document.Id == null)
                        ThrowInvalidData();

                    result.Documents.LastEtag = item.Document.Etag;

                    if (CanSkipDocument(item.Document, buildType))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_options.IncludeExpired == false &&
                        ExpirationStorage.HasPassed(item.Document.Data, _time.GetUtcNow()))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_options.IncludeArtificial == false && item.Document.Flags.HasFlag(DocumentFlags.Artificial))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_patcher != null)
                    {
                        item.Document = _patcher.Transform(item.Document);
                        if (item.Document == null)
                        {
                            result.Documents.SkippedCount++;
                            if (result.Documents.SkippedCount % 1000 == 0)
                                AddInfoToSmugglerResult(result, $"Skipped {result.Documents.SkippedCount:#,#;;0} documents.");
                            continue;
                        }
                    }

                    SetDocumentOrTombstoneFlags(ref item.Document.Flags, ref item.Document.NonPersistentFlags, buildType);

                    if (SkipDocument(buildType, isPreV4Revision, item, result, ref legacyIdsToDelete))
                        continue;
                    await actions.WriteDocumentAsync(item, result.Documents);
                }

                await TryHandleLegacyDocumentTombstonesAsync(legacyIdsToDelete, actions, result);
            }

            if (buildType == BuildVersionType.V3 && result.RevisionDocuments.ReadCount > 0)
                result.RevisionDocuments.Processed = true;

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessRevisionDocumentsAsync(SmugglerResult result)
        {
            result.RevisionDocuments.Start();

            await using (var actions = _destination.RevisionDocuments())
            {
                await foreach (var item in _source.GetRevisionDocumentsAsync(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.RevisionDocuments.ReadCount++;

                    if (result.RevisionDocuments.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.RevisionDocuments.ReadCount:#,#;;0} revision documents.");

                    if (item.Document == null)
                    {
                        result.RevisionDocuments.ErroredCount++;
                        continue;
                    }

                    Debug.Assert(item.Document.Id != null);

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    await actions.WriteDocumentAsync(item, result.RevisionDocuments);

                    result.RevisionDocuments.LastEtag = item.Document.Etag;
                }
            }

            return result.RevisionDocuments;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTombstonesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            result.Tombstones.Start();

            await using (var actions = _destination.Tombstones())
            {
                await foreach (var tombstone in _source.GetTombstonesAsync(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.Tombstones.ReadCount++;

                    if (result.Tombstones.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Tombstones.ReadCount:#,#;;0} tombstones.");

                    if (tombstone == null)
                    {
                        result.Tombstones.ErroredCount++;
                        continue;
                    }

                    if (tombstone.LowerId == null)
                        ThrowInvalidData();


                    if (_options.IncludeArtificial == false && tombstone.Flags.HasFlag(DocumentFlags.Artificial))
                    {
                        continue;
                    }

                    var _ = NonPersistentDocumentFlags.None;
                    SetDocumentOrTombstoneFlags(ref tombstone.Flags, ref _, buildType);

                    await actions.WriteTombstoneAsync(tombstone, result.Tombstones);

                    result.Tombstones.LastEtag = tombstone.Etag;
                }
            }

            return result.Tombstones;
        }

        protected override async Task ProcessDocumentsWithDuplicateCollectionAsync(SmugglerResult result)
        {
            var didWork = false;
            var count = 0;
            await using (var actions = _destination.Documents())
            {
                foreach (var item in actions.GetDocumentsWithDuplicateCollection())
                {
                    if (didWork == false)
                    {
                        result.AddInfo("Starting to process documents with duplicate collection.");
                        didWork = true;
                    }
                    await actions.WriteDocumentAsync(item, result.Documents);
                    count++;
                }
            }

            if (didWork)
                result.AddInfo($"Finished processing '{count}' documents with duplicate collection.");
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessConflictsAsync(SmugglerResult result)
        {
            result.Conflicts.Start();

            await using (var actions = _destination.Conflicts())
            {
                await foreach (var conflict in _source.GetConflictsAsync(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.Conflicts.ReadCount++;

                    if (result.Conflicts.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Conflicts.ReadCount:#,#;;0} conflicts.");

                    if (conflict == null)
                    {
                        result.Conflicts.ErroredCount++;
                        continue;
                    }

                    if (conflict.Id == null)
                        ThrowInvalidData();

                    await actions.WriteConflictAsync(conflict, result.Conflicts);

                    result.Conflicts.LastEtag = conflict.Etag;
                }
            }

            return result.Conflicts;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result)
        {
            result.Indexes.Start();

            await using (var actions = _destination.Indexes())
            {
                await foreach (var index in _source.GetIndexesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Indexes.ReadCount++;

                    if (index == null)
                    {
                        result.Indexes.ErroredCount++;
                        continue;
                    }

                    if (OnIndexAction != null)
                    {
                        OnIndexAction(index);
                        continue;
                    }

                    switch (index.Type)
                    {
                        case IndexType.AutoMap:
                            var autoMapIndexDefinition = (AutoMapIndexDefinition)index.IndexDefinition;

                            try
                            {
                                await actions.WriteIndexAsync(autoMapIndexDefinition, IndexType.AutoMap);
                            }
                            catch (Exception e)
                            {
                                result.Indexes.ErroredCount++;
                                result.AddError($"Could not write auto map index '{autoMapIndexDefinition.Name}': {e.Message}");
                            }
                            break;

                        case IndexType.AutoMapReduce:
                            var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)index.IndexDefinition;
                            try
                            {
                                await actions.WriteIndexAsync(autoMapReduceIndexDefinition, IndexType.AutoMapReduce);
                            }
                            catch (Exception e)
                            {
                                result.Indexes.ErroredCount++;
                                result.AddError($"Could not write auto map-reduce index '{autoMapReduceIndexDefinition.Name}': {e.Message}");
                            }
                            break;

                        case IndexType.Map:
                        case IndexType.MapReduce:
                        case IndexType.JavaScriptMap:
                        case IndexType.JavaScriptMapReduce:
                            var indexDefinition = (IndexDefinition)index.IndexDefinition;
                            if (string.Equals(indexDefinition.Name, "Raven/DocumentsByEntityName", StringComparison.OrdinalIgnoreCase))
                            {
                                result.AddInfo("Skipped 'Raven/DocumentsByEntityName' index. It is no longer needed.");
                                continue;
                            }

                            if (string.Equals(indexDefinition.Name, "Raven/ConflictDocuments", StringComparison.OrdinalIgnoreCase))
                            {
                                result.AddInfo("Skipped 'Raven/ConflictDocuments' index. It is no longer needed.");
                                continue;
                            }

                            if (indexDefinition.Name.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
                            {
                                // legacy auto index
                                indexDefinition.Name = $"Legacy/{indexDefinition.Name}";
                            }

                            await WriteIndexAsync(result, indexDefinition, actions);
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
            }

            return result.Indexes;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessIdentitiesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            return await ProcessIdentitiesInternalAsync(result, buildType, _destination.Identities());
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentsAsync(SmugglerResult result)
        {
            await using (var actions = _destination.Documents())
            {
                await foreach (var item in _source.GetLegacyAttachmentsAsync(actions))
                {
                    _token.ThrowIfCancellationRequested();

                    result.Documents.ReadCount++;
                    result.Documents.Attachments.ReadCount++;
                    if (result.Documents.Attachments.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Documents.Attachments.ReadCount:#,#;;0} legacy attachments.");

                    if (item.Document.Id == null)
                        ThrowInvalidData();

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    await actions.WriteDocumentAsync(item, result.Documents);

                    result.Documents.LastEtag = item.Document.Etag;
                }
            }

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentDeletionsAsync(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            await using (var actions = _destination.Documents())
            {
                await foreach (var id in _source.GetLegacyAttachmentDeletionsAsync())
                {
                    counts.ReadCount++;

                    if (counts.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {counts.ReadCount:#,#;;0} legacy attachment deletions.");

                    try
                    {
                        await actions.DeleteDocumentAsync(id);
                    }
                    catch (Exception e)
                    {
                        counts.ErroredCount++;
                        result.AddError($"Could not delete document (legacy attachment deletion) with id '{id}': {e.Message}");
                    }
                }
            }

            return counts;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyDocumentDeletionsAsync(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            await using (var actions = _destination.Documents())
            {
                await foreach (var id in _source.GetLegacyDocumentDeletionsAsync())
                {
                    counts.ReadCount++;

                    if (counts.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {counts.ReadCount:#,#;;0} legacy document deletions.");

                    try
                    {
                        await actions.DeleteDocumentAsync(id);
                    }
                    catch (Exception e)
                    {
                        counts.ErroredCount++;
                        result.AddError($"Could not delete document (legacy document deletion) with id '{id}': {e.Message}");
                    }
                }
            }

            return counts;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result)
        {
            bool isSharded = false;
            int index = -1;
            DatabaseRecord shardedRecord = null;
            result.CompareExchange.Start();

            //Handle compare exchange for shard database export
            if (_source is DatabaseSource)
            {
                var record = await _source.GetDatabaseRecordAsync();
                if (ShardHelper.IsShardedName(record.DatabaseName))
                {
                    isSharded = true;
                    shardedRecord = await _source.GetShardedDatabaseRecordAsync();
                    index = ShardHelper.TryGetShardIndex(record.DatabaseName);
                }
            }

            await using (var actions = _destination.CompareExchange(_context))
            {
                await foreach (var kvp in _source.GetCompareExchangeValuesAsync())
                {
                    if (SkipCompareExchange(isSharded, kvp.Key, index, shardedRecord))
                        continue;

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

        private bool SkipCompareExchange(bool isSharded, CompareExchangeKey Key, int index, DatabaseRecord databaseRecord)
        {
            if (isSharded)
            {
                if (ClusterTransactionCommand.IsAtomicGuardKey(Key.Key, out var docId))
                {
                    var id = docId.Split('/')[1];
                    using (_transactionContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var docShardIndex = ShardHelper.GetShardIndexforDocument(context, databaseRecord.ShardAllocations, docId);
                        if (docShardIndex != index)
                        {
                            return true;
                        }
                    }
                }
                else
                {
                    if (index != databaseRecord.ShardAllocations.Count - 1)
                        return true;
                }
            }

            return false;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCountersAsync(SmugglerResult result)
        {
            result.Counters.Start();

            await using (var actions = _destination.Counters(result))
            {
                await foreach (var counterGroup in _source.GetCounterValuesAsync(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.Counters.ReadCount++;

                    if (result.Counters.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Counters.ReadCount:#,#;;0} counters.");

                    if (_source is StreamSource streamSource)
                    {
                        foreach (var iDisposable in streamSource.ToDispose)
                        {
                            actions.RegisterForDisposal(iDisposable);
                        }
                    }

                    await actions.WriteCounterAsync(counterGroup);

                    result.Counters.LastEtag = counterGroup.Etag;
                }
            }

            return result.Counters;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessLegacyCountersAsync(SmugglerResult result)
        {
            await using (var actions = _destination.Counters(result))
            {
                await foreach (var counterDetail in _source.GetLegacyCounterValuesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Counters.ReadCount++;

                    if (result.Counters.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Counters.ReadCount:#,#;;0} counters.");

                    await actions.WriteLegacyCounterAsync(counterDetail);

                    result.Counters.LastEtag = counterDetail.Etag;
                }
            }

            return result.Counters;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result)
        {
            bool isSharded = false;
            int index = -1;
            DatabaseRecord shardedRecord = null;

            if (_source is DatabaseSource)
            {
                var record = await _source.GetDatabaseRecordAsync();
                if (ShardHelper.IsShardedName(record.DatabaseName))
                {
                    isSharded = true;
                    shardedRecord = await _source.GetShardedDatabaseRecordAsync();
                    index = ShardHelper.TryGetShardIndex(record.DatabaseName);
                }
            }

            result.CompareExchangeTombstones.Start();

            await using (var actions = _destination.CompareExchangeTombstones(_context))
            {
                await foreach (var key in _source.GetCompareExchangeTombstonesAsync())
                {
                    if (SkipCompareExchange(isSharded, key.Key, index, shardedRecord))
                        continue;

                    _token.ThrowIfCancellationRequested();
                    result.CompareExchangeTombstones.ReadCount++;

                    if (key.Equals(default))
                    {
                        result.CompareExchangeTombstones.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        await actions.WriteTombstoneKeyAsync(key.Key.Key);
                    }
                    catch (Exception e)
                    {
                        result.CompareExchangeTombstones.ErroredCount++;
                        result.AddError($"Could not write compare exchange '{key}: {e.Message}");
                    }
                }
            }

            return result.CompareExchangeTombstones;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessSubscriptionsAsync(SmugglerResult result)
        {
            return await ProcessSubscriptionsInternalAsync(result, _destination.Subscriptions());
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result)
        {
            return await ProcessReplicationHubCertificatesInternalAsync(result, _destination.ReplicationHubCertificates());
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTimeSeriesAsync(SmugglerResult result)
        {
            result.TimeSeries.Start();

            await using (var actions = _destination.TimeSeries())
            {
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var ts in _source.GetTimeSeriesAsync(_options.Collections))
                {
                    _token.ThrowIfCancellationRequested();
                    result.TimeSeries.ReadCount += ts.Segment.NumberOfEntries;

                    if (result.TimeSeries.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.TimeSeries.ReadCount:#,#;;0} time series.");

                    result.TimeSeries.LastEtag = ts.Etag;

                    var shouldSkip = isFullBackup && ts.Segment.NumberOfLiveEntries == 0;
                    if (shouldSkip == false)
                        await actions.WriteTimeSeriesAsync(ts);
                }
            }

            return result.TimeSeries;
        }

        protected async ValueTask WriteIndexAsync(SmugglerResult result, IndexDefinition indexDefinition, IIndexActions actions)
        {
            try
            {
                if (_options.RemoveAnalyzers)
                {
                    foreach (var indexDefinitionField in indexDefinition.Fields)
                        indexDefinitionField.Value.Analyzer = null;
                }

                await actions.WriteIndexAsync(indexDefinition);
            }
            catch (Exception e)
            {
                var exceptionMessage = e.Message;
                if (exceptionMessage.Contains("CS1501") && exceptionMessage.Contains("'LoadDocument'"))
                {
                    exceptionMessage =
                            "LoadDocument requires a second argument which is a collection name of the loaded document" + Environment.NewLine +
                            "For example: " + Environment.NewLine +
                                "\tfrom doc in doc.Orders" + Environment.NewLine +
                                "\tlet company = LoadDocument(doc.Company, \"Companies\")" + Environment.NewLine +
                                "\tselect new {" + Environment.NewLine +
                                    "\t\tCompanyName: company.Name" + Environment.NewLine +
                                "\t}" + Environment.NewLine +
                            exceptionMessage + Environment.NewLine;
                }
                else if (exceptionMessage.Contains("CS0103") &&
                         (exceptionMessage.Contains("'AbstractIndexCreationTask'") ||
                          exceptionMessage.Contains("'SpatialIndex'")))
                {
                    exceptionMessage = "'AbstractIndexCreationTask.SpatialGenerate' can be replaced with 'CreateSpatialField'" + Environment.NewLine +
                                       "'SpatialIndex.Generate' can be replaced with 'CreateSpatialField'" + Environment.NewLine +
                                       exceptionMessage + Environment.NewLine;
                }
                else if (exceptionMessage.Contains("CS0234") && exceptionMessage.Contains("'Abstractions'"))
                {
                    exceptionMessage = "'Raven.Abstractions.Linq.DynamicList' can be removed" + Environment.NewLine +
                                       $"{exceptionMessage}" + Environment.NewLine;
                }

                result.Indexes.ErroredCount++;
                var errorMessage = $"Could not write index '{indexDefinition.Name}', error: {exceptionMessage}" + Environment.NewLine +
                                   $"Maps: [{Environment.NewLine}{string.Join($", {Environment.NewLine}", indexDefinition.Maps)}{Environment.NewLine}]";

                if (indexDefinition.Reduce != null)
                {
                    errorMessage += Environment.NewLine + $"Reduce: {indexDefinition.Reduce}";
                }

                result.AddError(errorMessage);
            }
        }

    }
}
