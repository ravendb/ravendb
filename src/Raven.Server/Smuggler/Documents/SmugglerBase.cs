using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Smuggler.Documents
{
    public abstract class SmugglerBase
    {
        private readonly string _databaseName;
        internal readonly ISmugglerSource _source;
        internal readonly DatabaseSmugglerOptionsServerSide _options;
        internal readonly SmugglerResult _result;
        internal readonly SystemTime _time;
        internal readonly Action<IOperationProgress> _onProgress;
        internal readonly CancellationToken _token;
        protected readonly JsonOperationContext _context;
        public Action<DatabaseRecord> OnDatabaseRecordAction;
        public Action<IndexDefinitionAndType> OnIndexAction;
        public BackupKind BackupKind = BackupKind.None;
        internal readonly ISmugglerDestination _destination;
        private SmugglerPatcher _patcher;

        protected SmugglerBase(
            [NotNull] string databaseName,
            ISmugglerSource source,
            ISmugglerDestination destination,
            SystemTime time,
            JsonOperationContext context,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result = null,
            Action<IOperationProgress> onProgress = null,
            CancellationToken token = default)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _source = source;
            _options = options;
            _result = result;
            _token = token;
            _context = context;
            _time = time;
            _onProgress = onProgress ?? (progress => { });
            _destination = destination;
        }

        public abstract SmugglerPatcher CreatePatcher();

        /// <summary>
        /// isLastFile param true by default to correctly restore identities and compare exchange from V41 ravendbdump file.
        /// </summary>
        /// <param name="ensureStepsProcessed"></param>
        /// <param name="isLastFile"></param>
        /// <returns></returns>
        public virtual async Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            if (string.IsNullOrWhiteSpace(_options.TransformScript) == false)
                _patcher = CreatePatcher();

            var result = _result ?? new SmugglerResult();
            using (var initializeResult = await _source.InitializeAsync(_options, result))
            using (_patcher?.Initialize())
            await using (await _destination.InitializeAsync(_options, result, _onProgress, initializeResult.BuildNumber))
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

        internal void ModifyV41OperateOnTypes(long buildVersion, bool isLastFile)
        {
            if ((buildVersion < 42000 && buildVersion >= 40000) || buildVersion == 41 || buildVersion == 40)
            {
                if (isLastFile)
                {
                    // restore CompareExchange and Identities only from last file
                    _options.OperateOnTypes |= DatabaseItemType.CompareExchange;
                    _options.OperateOnTypes |= DatabaseItemType.Identities;

                    // there is no CompareExchangeTombstones in versions prior to 4.2
                    _options.OperateOnTypes &= ~DatabaseItemType.CompareExchangeTombstones;
                }
                else
                {
                    _options.OperateOnTypes &= ~DatabaseItemType.Identities;
                    _options.OperateOnTypes &= ~DatabaseItemType.CompareExchange;
                    _options.OperateOnTypes &= ~DatabaseItemType.CompareExchangeTombstones;
                }
            }
        }

        public static void EnsureProcessed(SmugglerResult result, bool skipped = true, bool? indexesSkipped = null)
        {
            EnsureStepProcessed(result.DatabaseRecord, skipped);
            EnsureStepProcessed(result.Documents, skipped);
            EnsureStepProcessed(result.Documents.Attachments, skipped);
            EnsureStepProcessed(result.RevisionDocuments, skipped);
            EnsureStepProcessed(result.RevisionDocuments.Attachments, skipped);
            EnsureStepProcessed(result.Counters, skipped);
            EnsureStepProcessed(result.Tombstones, skipped);
            EnsureStepProcessed(result.Conflicts, skipped);
            EnsureStepProcessed(result.Indexes, indexesSkipped ?? skipped);
            EnsureStepProcessed(result.Identities, skipped);
            EnsureStepProcessed(result.CompareExchange, skipped);
            EnsureStepProcessed(result.CompareExchangeTombstones, skipped);
            EnsureStepProcessed(result.Subscriptions, skipped);
            EnsureStepProcessed(result.TimeSeries, skipped);
            EnsureStepProcessed(result.ReplicationHubCertificates, skipped);
            EnsureStepProcessed(result.TimeSeriesDeletedRanges, skipped);

            static void EnsureStepProcessed(SmugglerProgressBase.Counts counts, bool skipped)
            {
                if (counts.Processed)
                    return;

                counts.Processed = true;
                counts.Skipped = skipped;
            }
        }

        protected virtual async Task ProcessTypeAsync(DatabaseItemType type, SmugglerResult result, BuildVersionType buildType, bool ensureStepsProcessed = true)
        {

            if ((_options.OperateOnTypes & type) != type)
            {
                switch (type)
                {
                    case DatabaseItemType.LegacyDocumentDeletions:
                        // process only those when we are processing documents
                        if ((_options.OperateOnTypes & DatabaseItemType.Documents) != DatabaseItemType.Documents)
                        {
                            await SkipTypeAsync(type, result, ensureStepsProcessed);
                            return;
                        }

                        break;

                    case DatabaseItemType.LegacyAttachments:
                    case DatabaseItemType.LegacyAttachmentDeletions:
                        // we cannot skip those?
                        break;

                    default:
                        await SkipTypeAsync(type, result, ensureStepsProcessed);
                        return;
                }
            }

            result.StartProcessingForType(type);
            _onProgress.Invoke(result.Progress);

            SmugglerProgressBase.Counts counts;
            switch (type)
            {
                case DatabaseItemType.DatabaseRecord:
                    counts = await ProcessDatabaseRecordAsync(result);
                    break;

                case DatabaseItemType.Documents:
                    counts = await ProcessDocumentsAsync(result, buildType);
                    break;

                case DatabaseItemType.RevisionDocuments:
                    counts = await ProcessRevisionDocumentsAsync(result);
                    break;

                case DatabaseItemType.Tombstones:
                    counts = await ProcessTombstonesAsync(result, buildType);
                    await ProcessDocumentsWithDuplicateCollectionAsync(result);
                    break;

                case DatabaseItemType.Conflicts:
                    counts = await ProcessConflictsAsync(result);
                    break;

                case DatabaseItemType.Indexes:
                    counts = await ProcessIndexesAsync(result);
                    break;

                case DatabaseItemType.Identities:
                    counts = await ProcessIdentitiesAsync(result, buildType);
                    break;

                case DatabaseItemType.LegacyAttachments:
                    counts = await ProcessLegacyAttachmentsAsync(result);
                    break;

                case DatabaseItemType.LegacyDocumentDeletions:
                    counts = await ProcessLegacyDocumentDeletionsAsync(result);
                    break;

                case DatabaseItemType.LegacyAttachmentDeletions:
                    counts = await ProcessLegacyAttachmentDeletionsAsync(result);
                    break;

                case DatabaseItemType.CompareExchange:
                    counts = await ProcessCompareExchangeAsync(result, _databaseName);
                    break;

                case DatabaseItemType.CounterGroups:
                    counts = await ProcessCountersAsync(result);
                    break;

                case DatabaseItemType.CompareExchangeTombstones:
                    counts = await ProcessCompareExchangeTombstonesAsync(result, _databaseName);
                    break;

                case DatabaseItemType.Subscriptions:
                    counts = await ProcessSubscriptionsAsync(result);
                    break;

                case DatabaseItemType.ReplicationHubCertificates:
                    counts = await ProcessReplicationHubCertificatesAsync(result);
                    break;

                case DatabaseItemType.TimeSeries:
                    counts = await ProcessTimeSeriesAsync(result);
                    break;

                case DatabaseItemType.TimeSeriesDeletedRanges:
                    counts = await ProcessTimeSeriesDeletedRangesAsync(result);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }

            if (ensureStepsProcessed)
            {
                counts.Processed = true;

                if (counts is SmugglerProgressBase.CountsWithLastEtagAndAttachments countsWithEtagAndAttachments)
                {
                    countsWithEtagAndAttachments.Attachments.Processed = true;

                    switch (type)
                    {
                        case DatabaseItemType.Documents:
                        case DatabaseItemType.RevisionDocuments:
                            countsWithEtagAndAttachments.Attachments.Skipped = _options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false;
                            break;
                    }

                    if (buildType == BuildVersionType.V3 && type == DatabaseItemType.Documents && result.RevisionDocuments.ReadCount > 0)
                        result.RevisionDocuments.Processed = true;
                }
            }


            result.StopProcessingActualType(counts);
            _onProgress.Invoke(result.Progress);
        }

        internal async Task SkipTypeAsync(DatabaseItemType type, SmugglerResult result, bool ensureStepProcessed = true)
        {
            result.AddInfo($"Skipping '{type}' processing.");
            _onProgress.Invoke(result.Progress);

            SmugglerProgressBase.Counts counts;
            switch (type)
            {
                case DatabaseItemType.DatabaseRecord:
                    counts = result.DatabaseRecord;
                    break;

                case DatabaseItemType.Documents:
                    counts = result.Documents;
                    break;

                case DatabaseItemType.RevisionDocuments:
                    counts = result.RevisionDocuments;
                    break;

                case DatabaseItemType.Tombstones:
                    counts = result.Tombstones;
                    break;

                case DatabaseItemType.Conflicts:
                    counts = result.Conflicts;
                    break;

                case DatabaseItemType.Indexes:
                    counts = result.Indexes;
                    break;

                case DatabaseItemType.Identities:
                    counts = result.Identities;
                    break;

                case DatabaseItemType.CompareExchange:
                    counts = result.CompareExchange;
                    break;

                case DatabaseItemType.CounterGroups:
                    counts = result.Counters;
                    break;

                case DatabaseItemType.CompareExchangeTombstones:
                    counts = result.CompareExchangeTombstones;
                    break;

                case DatabaseItemType.Subscriptions:
                    counts = result.Subscriptions;
                    break;

                case DatabaseItemType.LegacyDocumentDeletions:
                    counts = new SmugglerProgressBase.Counts();
                    break;

                case DatabaseItemType.TimeSeries:
                    counts = result.TimeSeries;
                    break;

                case DatabaseItemType.ReplicationHubCertificates:
                    counts = result.ReplicationHubCertificates;
                    break;

                case DatabaseItemType.TimeSeriesDeletedRanges:
                    counts = result.TimeSeriesDeletedRanges;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }

            void OnSkipped(long skipped)
            {
                if (ensureStepProcessed == false)
                    return;

                if (type == DatabaseItemType.Documents)
                    result.Documents.SkippedCount = skipped;

                if (skipped % 10000 != 0)
                    return;

                result.AddInfo($"Skipped {skipped:#,#;;0} {type.ToString().ToLowerInvariant()}");
                _onProgress.Invoke(result.Progress);
            }

            var numberOfItemsSkipped = await _source.SkipTypeAsync(type, OnSkipped, _token);

            if (ensureStepProcessed == false)
                return;

            counts.Skipped = true;
            counts.Processed = true;

            if (numberOfItemsSkipped > 0)
            {
                counts.ReadCount = numberOfItemsSkipped;
                result.AddInfo($"Skipped '{type}' processing. Skipped {numberOfItemsSkipped:#,#;;0} items.");
            }
            else
                result.AddInfo($"Skipped '{type}' processing.");

            _onProgress.Invoke(result.Progress);
        }

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessDocumentsAsync(SmugglerResult result, BuildVersionType buildType)
        {
            result.Documents.Start();

            var throwOnCollectionMismatchError = _options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false;

            await using (var documentActions = _destination.Documents(throwOnCollectionMismatchError))
            await using (var compareExchangeActions = _destination.CompareExchange(_databaseName, _context, BackupKind, withDocuments: true))
            {
                List<LazyStringValue> legacyIdsToDelete = null;
                Func<ValueTask> beforeFlush = compareExchangeActions == null ? null : compareExchangeActions.FlushAsync;

                await foreach (DocumentItem item in _source.GetDocumentsAsync(_options.Collections, documentActions))
                {
                    _token.ThrowIfCancellationRequested();

                    var isPreV4Revision = DatabaseSmuggler.IsPreV4Revision(buildType, item.Document.Id, item.Document);
                    if (isPreV4Revision)
                    {
                        result.RevisionDocuments.ReadCount++;
                        result.RevisionDocuments.SizeInBytes += item.Document.Data.Size;
                    }
                    else
                    {
                        result.Documents.ReadCount++;
                        result.Documents.SizeInBytes += item.Document.Data.Size;
                    }

                    if (item.Attachments != null)
                    {
                        foreach (var attachment in item.Attachments)
                        {
                            if (attachment.Stream != null)
                            {
                                if (isPreV4Revision)
                                {
                                    result.RevisionDocuments.Attachments.ReadCount++;
                                    result.RevisionDocuments.Attachments.SizeInBytes += attachment.Stream.Length;
                                }
                                else
                                {
                                    result.Documents.Attachments.ReadCount++;
                                    result.Documents.Attachments.SizeInBytes += attachment.Stream.Length;
                                }
                            }
                        }
                    }

                    if (result.Documents.ReadCount % 1000 == 0)
                    {
                        var message = $"Documents: {result.Documents}";
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
                        SkipDocument(item, result.Documents);
                        continue;
                    }

                    if (_options.IncludeExpired == false)
                    {
                        if (item.Document.Data.TryGetMetadata(out var metadata) &&
                            AbstractBackgroundWorkStorage.HasPassed(metadata, _time.GetUtcNow(), Constants.Documents.Metadata.Expires))
                        {
                            SkipDocument(item, result.Documents);
                            continue;
                        }
                    }
                    
                    if (_options.IncludeArtificial == false && item.Document.Flags.HasFlag(DocumentFlags.Artificial))
                    {
                        SkipDocument(item, result.Documents);
                        continue;
                    }
                    
                    if (_options.IncludeArchived == false && item.Document.Flags.HasFlag(DocumentFlags.Archived))
                    {
                        SkipDocument(item, result.Documents);
                        continue;
                    }

                    if (_patcher != null)
                    {
                        var patchedDocument = _patcher.Transform(item.Document);
                        if (patchedDocument == null)
                        {
                            SkipDocument(item, result.Documents);

                            if (result.Documents.SkippedCount % 1000 == 0)
                                AddInfoToSmugglerResult(result, $"Skipped {result.Documents.SkippedCount:#,#;;0} documents.");
                            continue;
                        }

                        item.Document = patchedDocument;
                    }

                    SetDocumentOrTombstoneFlags(ref item.Document.Flags, ref item.Document.NonPersistentFlags, buildType);

                    if (SkipDocument(buildType, isPreV4Revision, item, result, ref legacyIdsToDelete))
                        continue;

                    if (compareExchangeActions != null && item.Document.ChangeVector != null && item.Document.ChangeVector.Contains(ChangeVectorParser.TrxnTag))
                    {
                        var key = ClusterWideTransactionHelper.GetAtomicGuardKey(item.Document.Id);
                        await compareExchangeActions.WriteKeyValueAsync(key, null, item.Document);
                        continue;
                    }

                    await documentActions.WriteDocumentAsync(item, result.Documents, beforeFlush);
                }

                await TryHandleLegacyDocumentTombstonesAsync(legacyIdsToDelete, documentActions, result);
            }

            if (buildType == BuildVersionType.V3 && result.RevisionDocuments.ReadCount > 0)
                result.RevisionDocuments.Processed = true;

            return result.Documents;
        }

        protected virtual Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result) =>
            ProcessDatabaseRecordInternalAsync(result, _destination.DatabaseRecord());

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessRevisionDocumentsAsync(SmugglerResult result)
        {
            result.RevisionDocuments.Start();

            await using (var actions = _destination.RevisionDocuments())
            {
                await foreach (var item in _source.GetRevisionDocumentsAsync(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.RevisionDocuments.ReadCount++;

                    if (item.Document != null)
                        result.RevisionDocuments.SizeInBytes += item.Document.Data.Size;

                    if (item.Attachments != null)
                    {
                        foreach (var attachment in item.Attachments)
                        {
                            if (attachment.Stream != null)
                            {
                                result.RevisionDocuments.Attachments.ReadCount++;
                                result.RevisionDocuments.Attachments.SizeInBytes += attachment.Stream.Length;
                            }
                        }
                    }

                    if (result.RevisionDocuments.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Revisions {result.RevisionDocuments}");

                    if (item.Document == null)
                    {
                        result.RevisionDocuments.ErroredCount++;
                        continue;
                    }

                    Debug.Assert(item.Document.Id != null);

                    var shouldSkip = ShouldSkip(item, _patcher);

                    if (shouldSkip == false)
                    {
                        item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;
                        await actions.WriteDocumentAsync(item, result.RevisionDocuments);
                    }
                    else
                    {
                        SkipDocument(item, result.RevisionDocuments);
                    }

                    result.RevisionDocuments.LastEtag = item.Document.Etag;
                }
            }

            return result.RevisionDocuments;

            static bool ShouldSkip(DocumentItem item, SmugglerPatcher patcher)
            {
                return patcher != null && patcher.ShouldSkip(item.Document.Id);
            }
        }

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessTombstonesAsync(SmugglerResult result, BuildVersionType buildType)
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

                    if (_options.IncludeArtificial == false &&
                        tombstone.Flags.Contain(DocumentFlags.Artificial) &&
                        tombstone.Flags.Contain(DocumentFlags.FromResharding) == false)
                    {
                        continue;
                    }
                    
                    if (_options.IncludeArchived == false && // skip archived tombstones - we don't track archived data
                        tombstone.Flags.Contain(DocumentFlags.Archived))
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

        protected virtual async Task ProcessDocumentsWithDuplicateCollectionAsync(SmugglerResult result)
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

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessConflictsAsync(SmugglerResult result)
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

        private async Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result)
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

                    if (ShouldSkipIndex(index, out var msg))
                    {
                        result.AddWarning(msg);
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
                                await actions.WriteAutoIndexAsync(autoMapIndexDefinition, IndexType.AutoMap, _options.AuthorizationStatus);
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
                                await actions.WriteAutoIndexAsync(autoMapReduceIndexDefinition, IndexType.AutoMapReduce, _options.AuthorizationStatus);
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

        protected virtual bool ShouldSkipIndex(IndexDefinitionAndType index, out string msg)
        {
            msg = null;
            return false;
        }

        protected virtual Task<SmugglerProgressBase.Counts> ProcessIdentitiesAsync(SmugglerResult result, BuildVersionType buildType) =>
            ProcessIdentitiesInternalAsync(result, buildType, _destination.Identities());

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentsAsync(SmugglerResult result)
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

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentDeletionsAsync(SmugglerResult result)
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

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessLegacyDocumentDeletionsAsync(SmugglerResult result)
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

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessCountersAsync(SmugglerResult result)
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

                    var shouldSkip = ShouldSkip(counterGroup, _patcher);

                    if (shouldSkip == false)
                        await actions.WriteCounterAsync(counterGroup);
                    else
                    {
                        result.Counters.SkippedCount++;
                    }

                    result.Counters.LastEtag = counterGroup.Etag;
                }
            }

            return result.Counters;

            static bool ShouldSkip(CounterGroupDetail counterGroup, SmugglerPatcher patcher)
            {
                return patcher != null && patcher.ShouldSkip(counterGroup.DocumentId);
            }
        }

        protected virtual Task<SmugglerProgressBase.Counts> ProcessSubscriptionsAsync(SmugglerResult result) =>
            ProcessSubscriptionsInternalAsync(result, _destination.Subscriptions());

        protected virtual Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result) =>
            ProcessReplicationHubCertificatesInternalAsync(result, _destination.ReplicationHubCertificates());

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessTimeSeriesAsync(SmugglerResult result)
        {
            result.TimeSeries.Start();

            await using (var actions = _destination.TimeSeries())
            {
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var ts in _source.GetTimeSeriesAsync(actions, _options.Collections))
                {
                    _token.ThrowIfCancellationRequested();
                    result.TimeSeries.ReadCount += ts.Segment.NumberOfEntries;
                    result.TimeSeries.SizeInBytes += ts.Segment.NumberOfBytes;

                    if (result.TimeSeries.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Time Series entries {result.TimeSeries}");

                    var shouldSkip = ShouldSkip(ts, _patcher, isFullBackup);

                    if (shouldSkip == false)
                        await actions.WriteTimeSeriesAsync(ts);
                    else
                    {
                        result.TimeSeries.SkippedCount += ts.Segment.NumberOfEntries;
                    }

                    result.TimeSeries.LastEtag = ts.Etag;
                }
            }

            return result.TimeSeries;

            static bool ShouldSkip(TimeSeriesItem ts, SmugglerPatcher patcher, bool isFullBackup)
            {
                if (isFullBackup && ts.Segment.NumberOfLiveEntries == 0)
                    return true;

                return patcher != null && patcher.ShouldSkip(ts.DocId);
            }
        }

        protected virtual async Task<SmugglerProgressBase.Counts> ProcessTimeSeriesDeletedRangesAsync(SmugglerResult result)
        {
            result.TimeSeriesDeletedRanges.Start();

            await using (var actions = _destination.TimeSeriesDeletedRanges())
            {
                await foreach (var deletedRange in _source.GetTimeSeriesDeletedRangesAsync(actions, _options.Collections))
                {
                    _token.ThrowIfCancellationRequested();
                    result.TimeSeriesDeletedRanges.ReadCount++;

                    if (result.TimeSeriesDeletedRanges.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Time Series deleted ranges entries {result.TimeSeriesDeletedRanges}");

                    if (ShouldSkip(deletedRange, _patcher) == false)
                        await actions.WriteTimeSeriesDeletedRangeAsync(deletedRange);
                    else
                        result.TimeSeriesDeletedRanges.SkippedCount++;

                    result.TimeSeriesDeletedRanges.LastEtag = deletedRange.Etag;
                }
            }

            return result.TimeSeriesDeletedRanges;

            static bool ShouldSkip(TimeSeriesDeletedRangeItemForSmuggler deletedRange, SmugglerPatcher patcher)
            {
                return patcher != null && patcher.ShouldSkip(deletedRange.DocId);
            }
        }

        protected async Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordInternalAsync(SmugglerResult result, IDatabaseRecordActions action)
        {
            result.DatabaseRecord.Start();

            await using (var actions = action)
            {
                var databaseRecord = await _source.GetDatabaseRecordAsync();

                _token.ThrowIfCancellationRequested();

                if (OnDatabaseRecordAction != null)
                {
                    OnDatabaseRecordAction(databaseRecord);
                    return new SmugglerProgressBase.DatabaseRecordProgress();
                }

                result.DatabaseRecord.ReadCount++;

                SkipDatabaseRecordTypesIfNeeded(databaseRecord, result, _options.OperateOnDatabaseRecordTypes);

                try
                {
                    await actions.WriteDatabaseRecordAsync(databaseRecord, result, _options.AuthorizationStatus, _options.OperateOnDatabaseRecordTypes);
                }
                catch (LicenseLimitException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    result.AddError($"Could not write database record: {e.Message}");
                    result.DatabaseRecord.ErroredCount++;
                }
            }

            return result.DatabaseRecord;
        }

        protected virtual void SkipDatabaseRecordTypesIfNeeded(DatabaseRecord databaseRecord, SmugglerResult result, DatabaseRecordItemType databaseRecordItemType)
        {
        }

        protected async Task<SmugglerProgressBase.Counts> ProcessIdentitiesInternalAsync(SmugglerResult result, BuildVersionType buildType, IKeyValueActions<long> action)
        {
            result.Identities.Start();

            await using (var actions = action)
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

        protected async Task<SmugglerProgressBase.Counts> ProcessSubscriptionsInternalAsync(SmugglerResult result, ISubscriptionActions action)
        {
            result.Subscriptions.Start();

            await using (var actions = action)
            {
                await foreach (var subscription in _source.GetSubscriptionsAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Subscriptions.ReadCount++;

                    if (result.Subscriptions.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Subscriptions.ReadCount:#,#;;0} subscription.");

                    await actions.WriteSubscriptionAsync(subscription);
                }
            }

            return result.Subscriptions;
        }

        protected async Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesInternalAsync(SmugglerResult result, IReplicationHubCertificateActions action)
        {
            result.ReplicationHubCertificates.Start();

            await using (var actions = action)
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

        protected async Task TryHandleLegacyDocumentTombstonesAsync(List<LazyStringValue> legacyIdsToDelete, IDocumentActions actions, SmugglerResult result)
        {
            if (legacyIdsToDelete == null)
                return;

            foreach (var idToDelete in legacyIdsToDelete)
            {
                await actions.DeleteDocumentAsync(idToDelete);

                result.Tombstones.ReadCount++;
                if (result.Tombstones.ReadCount % 1000 == 0)
                    AddInfoToSmugglerResult(result, $"Read {result.Tombstones.ReadCount:#,#;;0} tombstones.");
            }
        }


        protected void AddInfoToSmugglerResult(SmugglerResult result, string message)
        {
            result.AddInfo(message);
            _onProgress.Invoke(result.Progress);
        }

        protected void SetDocumentOrTombstoneFlags(ref DocumentFlags flags, ref NonPersistentDocumentFlags nonPersistentFlags, BuildVersionType buildType)
        {
            flags = flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.FromReplication);
            nonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

            if (_options.SkipRevisionCreation)
                nonPersistentFlags |= NonPersistentDocumentFlags.SkipRevisionCreationForSmuggler;

            switch (buildType)
            {
                case BuildVersionType.V4:
                case BuildVersionType.V5:
                case BuildVersionType.V6:
                case BuildVersionType.GreaterThanCurrent:
                    {
                        if (_options.OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments) == false)
                            flags = flags.Strip(DocumentFlags.HasRevisions);

                        // those flags will be re-added once counter/time-series is imported
                        flags = flags.Strip(DocumentFlags.HasCounters);
                        flags = flags.Strip(DocumentFlags.HasTimeSeries);

                        // attachments are special because they are referenced
                        if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false)
                            flags = flags.Strip(DocumentFlags.HasAttachments);

                        break;
                    }
            }
        }

        protected static void SkipDocument(DocumentItem item, SmugglerProgressBase.CountsWithSkippedCountAndLastEtagAndAttachments counts)
        {
            counts.SkippedCount++;

            if (item.Document != null)
            {
                item.Document.Dispose();

                if (item.Attachments != null)
                {
                    foreach (var attachment in item.Attachments)
                    {
                        attachment.Dispose();
                    }
                }
            }
        }

        protected bool SkipDocument(BuildVersionType buildType, bool isPreV4Revision, DocumentItem item, SmugglerResult result, ref List<LazyStringValue> legacyIdsToDelete)
        {
            if (buildType == BuildVersionType.V3 == false)
                return false;

            if (_options.OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments) == false && isPreV4Revision)
            {
                result.Documents.SkippedCount++;
                if (result.Documents.SkippedCount % 1000 == 0)
                    AddInfoToSmugglerResult(result, $"Skipped {result.Documents.SkippedCount:#,#;;0} documents.");

                return true;
            }

            if ((item.Document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyDeleteMarker) == NonPersistentDocumentFlags.LegacyDeleteMarker)
            {
                legacyIdsToDelete ??= new List<LazyStringValue>();
                legacyIdsToDelete.Add(item.Document.Id);
                return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static bool CanSkipDocument(Document document, BuildVersionType buildType)
        {
            if (buildType != BuildVersionType.V3)
                return false;

            // skipping
            // "Raven/Replication/DatabaseIdsCache" and
            // "Raven/Replication/Sources/{GUID}" and
            // "Raven/Replication/Destinations" and
            // "Raven/Backup/Periodic/Setup" and
            // "Raven/Backup/Status" and
            // "Raven/Backup/Periodic/Status"
            if (document.Id.Size != 34 && document.Id.Size != 62 &&
                document.Id.Size != 30 && document.Id.Size != 27 &&
                document.Id.Size != 19 && document.Id.Size != 28)
                return false;

            if (document.Id.StartsWith("Raven/") == false)
                return false;

            return document.Id == "Raven/Replication/DatabaseIdsCache" ||
                   document.Id == "Raven/Backup/Periodic/Setup" ||
                   document.Id == "Raven/Replication/Destinations" ||
                   document.Id == "Raven/Backup/Status" ||
                   document.Id == "Raven/Backup/Periodic/Status" ||
                   document.Id.StartsWith("Raven/Replication/Sources/");
        }

        [DoesNotReturn]
        protected static void ThrowInvalidData()
        {
            throw new InvalidDataException("Document does not contain an id.");
        }

        protected abstract Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result, string databaseName);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result, string databaseName);

        private async ValueTask WriteIndexAsync(SmugglerResult result, IndexDefinition indexDefinition, IIndexActions actions)
        {
            try
            {
                if (_options.RemoveAnalyzers)
                {
                    foreach (var indexDefinitionField in indexDefinition.Fields)
                        indexDefinitionField.Value.Analyzer = null;
                }

                await actions.WriteIndexAsync(indexDefinition, _options.AuthorizationStatus);
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
