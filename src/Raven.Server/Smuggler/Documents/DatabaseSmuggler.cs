using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSmuggler
    {
        private readonly DocumentDatabase _database;
        private readonly ISmugglerSource _source;
        private readonly ISmugglerDestination _destination;
        private readonly DatabaseSmugglerOptionsServerSide _options;
        private readonly SmugglerResult _result;
        private readonly SystemTime _time;
        private readonly Action<IOperationProgress> _onProgress;
        private readonly SmugglerPatcher _patcher;
        private readonly CancellationToken _token;

        public Action<IndexDefinitionAndType> OnIndexAction;
        public Action<DatabaseRecord> OnDatabaseRecordAction;

        public DatabaseSmuggler(DocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time,
            DatabaseSmugglerOptionsServerSide options = null, SmugglerResult result = null, Action<IOperationProgress> onProgress = null,
            CancellationToken token = default)
        {
            _database = database;
            _source = source;
            _destination = destination;
            _options = options ?? new DatabaseSmugglerOptionsServerSide();
            _result = result;
            _token = token;

            if (string.IsNullOrWhiteSpace(_options.TransformScript) == false)
                _patcher = new SmugglerPatcher(_options, database);

            _time = time;
            _onProgress = onProgress ?? (progress => { });
        }

        public SmugglerResult Execute(bool ensureStepsProcessed = true)
        {
            var result = _result ?? new SmugglerResult();

            using (_patcher?.Initialize())
            using (_source.Initialize(_options, result, out long buildVersion))
            using (_destination.Initialize(_options, result, buildVersion))
            {
                var buildType = BuildVersion.Type(buildVersion);
                var currentType = _source.GetNextType();
                while (currentType != DatabaseItemType.None)
                {
                    ProcessType(currentType, result, buildType, ensureStepsProcessed);

                    currentType = _source.GetNextType();
                }

                if (ensureStepsProcessed)
                {
                    EnsureProcessed(result);
                }

                return result;
            }
        }

        public static void EnsureProcessed(SmugglerResult result)
        {
            EnsureStepProcessed(result.DatabaseRecord);
            EnsureStepProcessed(result.Documents);
            EnsureStepProcessed(result.Documents.Attachments);
            EnsureStepProcessed(result.RevisionDocuments);
            EnsureStepProcessed(result.RevisionDocuments.Attachments);
            EnsureStepProcessed(result.Counters);
            EnsureStepProcessed(result.Tombstones);
            EnsureStepProcessed(result.Conflicts);
            EnsureStepProcessed(result.Indexes);
            EnsureStepProcessed(result.Identities);
            EnsureStepProcessed(result.CompareExchange);
        }

        private static void EnsureStepProcessed(SmugglerProgressBase.Counts counts)
        {
            if (counts.Processed)
                return;

            counts.Processed = true;
            counts.Skipped = true;
        }

        private void ProcessType(DatabaseItemType type, SmugglerResult result, BuildVersionType buildType, bool ensureStepsProcessed = true)
        {
            if ((_options.OperateOnTypes & type) != type)
            {
                switch (type)
                {
                    case DatabaseItemType.LegacyDocumentDeletions:
                        // process only those when we are processing documents
                        if ((_options.OperateOnTypes & DatabaseItemType.Documents) != DatabaseItemType.Documents)
                        {
                            SkipType(type, result, ensureStepsProcessed);
                            return;
                        }
                        break;
                    case DatabaseItemType.LegacyAttachments:
                    case DatabaseItemType.LegacyAttachmentDeletions:
                        // we cannot skip those?
                        break;
                    default:
                        SkipType(type, result, ensureStepsProcessed);
                        return;
                }
            }

            result.AddInfo($"Started processing {type}.");
            _onProgress.Invoke(result.Progress);

            SmugglerProgressBase.Counts counts;
            switch (type)
            {
                case DatabaseItemType.DatabaseRecord:
                    counts = ProcessDatabaseRecord(result);
                    break;
                case DatabaseItemType.Documents:
                    counts = ProcessDocuments(result, buildType);
                    break;
                case DatabaseItemType.RevisionDocuments:
                    counts = ProcessRevisionDocuments(result);
                    break;
                case DatabaseItemType.Tombstones:
                    counts = ProcessTombstones(result);
                    break;
                case DatabaseItemType.Conflicts:
                    counts = ProcessConflicts(result);
                    break;
                case DatabaseItemType.Indexes:
                    counts = ProcessIndexes(result);
                    break;
                case DatabaseItemType.Identities:
                    counts = ProcessIdentities(result);
                    break;
                case DatabaseItemType.LegacyAttachments:
                    counts = ProcessLegacyAttachments(result);
                    break;
                case DatabaseItemType.LegacyDocumentDeletions:
                    counts = ProcessLegacyDocumentDeletions(result);
                    break;
                case DatabaseItemType.LegacyAttachmentDeletions:
                    counts = ProcessLegacyAttachmentDeletions(result);
                    break;
                case DatabaseItemType.CompareExchange:
                    counts = ProcessCompareExchange(result);
                    break;
                case DatabaseItemType.Counters:
                    counts = ProcessCounters(result);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }

            if (ensureStepsProcessed)
            {
                counts.Processed = true;

                if (counts is SmugglerProgressBase.CountsWithLastEtag countsWithEtag)
                {
                    countsWithEtag.Attachments.Processed = true;
                }
            }

            result.AddInfo($"Finished processing {type}. {counts}");
            _onProgress.Invoke(result.Progress);
        }

        private void SkipType(DatabaseItemType type, SmugglerResult result, bool ensureStepProcessed = true)
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
                case DatabaseItemType.Counters:
                    counts = result.Counters;
                    break;
                case DatabaseItemType.LegacyDocumentDeletions:
                    counts = new SmugglerProgressBase.Counts();
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

            var numberOfItemsSkipped = _source.SkipType(type, OnSkipped, _token);

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

        private SmugglerProgressBase.Counts ProcessIdentities(SmugglerResult result)
        {
            using (var actions = _destination.Identities())
            {
                foreach (var kvp in _source.GetIdentities())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Identities.ReadCount++;

                    if (kvp.Equals(default))
                    {
                        result.Identities.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        actions.WriteKeyValue(kvp.Prefix, kvp.Value);
                    }
                    catch (Exception e)
                    {
                        result.Identities.ErroredCount++;
                        result.AddError($"Could not write identity '{kvp.Prefix}->{kvp.Value}': {e.Message}");
                    }
                }
            }

            return result.Identities;
        }

        private SmugglerProgressBase.Counts ProcessIndexes(SmugglerResult result)
        {
            using (var actions = _destination.Indexes())
            {
                foreach (var index in _source.GetIndexes())
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
                                actions.WriteIndex(autoMapIndexDefinition, IndexType.AutoMap);
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
                                actions.WriteIndex(autoMapReduceIndexDefinition, IndexType.AutoMapReduce);
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

                            WriteIndex(result, indexDefinition, actions);
                            break;
                        case IndexType.Faulty:
                            break;
                        default:
                            throw new NotSupportedException(index.Type.ToString());
                    }
                }
            }

            return result.Indexes;
        }

        private void WriteIndex(SmugglerResult result, IndexDefinition indexDefinition, IIndexActions actions)
        {
            try
            {
                if (_options.RemoveAnalyzers)
                {
                    foreach (var indexDefinitionField in indexDefinition.Fields)
                        indexDefinitionField.Value.Analyzer = null;
                }

                actions.WriteIndex(indexDefinition);
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

        private SmugglerProgressBase.DatabaseRecordProgress ProcessDatabaseRecord(SmugglerResult result)
        {
            using (var actions = _destination.DatabaseRecord())
            {
                var databaseRecord = _source.GetDatabaseRecord();

                _token.ThrowIfCancellationRequested();

                if (OnDatabaseRecordAction != null)
                {
                    OnDatabaseRecordAction(databaseRecord);
                    return new SmugglerProgressBase.DatabaseRecordProgress();
                }

                try
                {
                    actions.WriteDatabaseRecord(databaseRecord, result.DatabaseRecord, _options.AuthorizationStatus);
                }
                catch (Exception e)
                {
                    result.AddError($"Could not write database record: {e.Message}");
                }
            }

            return result.DatabaseRecord;
        }

        private SmugglerProgressBase.Counts ProcessRevisionDocuments(SmugglerResult result)
        {
            using (var actions = _destination.RevisionDocuments())
            {
                foreach (var item in _source.GetRevisionDocuments(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.RevisionDocuments.ReadCount++;

                    if (result.RevisionDocuments.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.RevisionDocuments.ReadCount:#,#;;0} documents.");

                    if (item.Document == null)
                    {
                        result.RevisionDocuments.ErroredCount++;
                        continue;
                    }

                    Debug.Assert(item.Document.Id != null);

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    actions.WriteDocument(item, result.RevisionDocuments);

                    result.RevisionDocuments.LastEtag = item.Document.Etag;
                }
            }

            return result.RevisionDocuments;
        }

        private SmugglerProgressBase.Counts ProcessDocuments(SmugglerResult result, BuildVersionType buildType)
        {
            using (var actions = _destination.Documents())
            {
                foreach (DocumentItem item in _source.GetDocuments(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();

                    if (item.Document == null)
                    {
                        result.Documents.ErroredCount++;
                        result.Documents.ReadCount--;
                        if (result.Documents.ErroredCount % 1000 == 0)
                            AddInfoToSmugglerResult(result, $"Error Count: {result.Documents.ErroredCount:#,#;;0}.");
                        continue;
                    }

                    if (item.Document.Id == null)
                        ThrowInvalidData();

                    if (CanSkipDocument(item.Document, buildType))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_options.IncludeExpired == false &&
                        ExpirationStorage.HasExpired(item.Document.Data, _time.GetUtcNow()))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_patcher != null)
                    {
                        item.Document = _patcher.Transform(item.Document, actions.GetContextForNewDocument());
                        if (item.Document == null)
                        {
                            result.Documents.SkippedCount++;
                            result.Documents.ReadCount--;
                            if (result.Documents.SkippedCount % 1000 == 0)
                                AddInfoToSmugglerResult(result, $"Skipped {result.Documents.SkippedCount:#,#;;0} documents.");
                            continue;
                        }
                    }

                    item.Document.Flags = item.Document.Flags.Strip(DocumentFlags.FromClusterTransaction);
                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    SetNonPersistentFlagsIfNeeded(buildType, item, result);

                    actions.WriteDocument(item, result.Documents);

                    result.Documents.LastEtag = item.Document.Etag;

                    result.Documents.ReadCount++;

                    var totalCount = result.Documents.ReadCount + result.Documents.SkippedCount + result.Documents.ErroredCount;
                    if (totalCount % 1000 == 0)
                    {
                        var message = $"Total processed {totalCount:#,#;;0} documents.";
                        if (result.Documents.Attachments.ReadCount > 0)
                            message += $" Read {result.Documents.Attachments.ReadCount:#,#;;0} attachments.";
                        AddInfoToSmugglerResult(result, message);
                    }
                }
            }

            return result.Documents;
        }

        private void AddInfoToSmugglerResult(SmugglerResult result, string message)
        {
            result.AddInfo(message);
            _onProgress.Invoke(result.Progress);
        }

        private void SetNonPersistentFlagsIfNeeded(BuildVersionType buildType, DocumentItem item, SmugglerResult result)
        {
            if (_options.SkipRevisionCreation)
            {
                item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.SkipRevisionCreation;
            }

            if (buildType == BuildVersionType.V3 && _options.OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments) == false)
            {
                item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.SkipLegacyRevision;

                if ((item.Document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) == NonPersistentDocumentFlags.LegacyRevision &&
                    item.Document.Id.Contains(DatabaseDestination.MergedBatchPutCommand.PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase))
                {
                    result.Documents.SkippedCount++;
                    result.Documents.ReadCount--;
                    if (result.Documents.SkippedCount % 1000 == 0)
                        AddInfoToSmugglerResult(result,$"Skipped {result.Documents.SkippedCount:#,#;;0} legacy revisions.");
                }
            }
        }

        private SmugglerProgressBase.Counts ProcessCompareExchange(SmugglerResult result)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var actions = _destination.CompareExchange(context))
            {
                foreach (var kvp in _source.GetCompareExchangeValues())
                {
                    _token.ThrowIfCancellationRequested();
                    result.CompareExchange.ReadCount++;

                    if (kvp.Equals(default))
                    {
                        result.CompareExchange.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        actions.WriteKeyValue(kvp.key, kvp.value);
                    }
                    catch (Exception e)
                    {
                        result.CompareExchange.ErroredCount++;
                        result.AddError($"Could not write compare exchange '{kvp.key}->{kvp.value}': {e.Message}");
                    }
                }
            }

            return result.CompareExchange;
        }

        private SmugglerProgressBase.Counts ProcessCounters(SmugglerResult result)
        {
            using (var actions = _destination.Counters())
            {
                foreach (var counterDetail in _source.GetCounterValues())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Counters.ReadCount++;

                    if (result.Counters.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Counters.ReadCount:#,#;;0} counters.");

                    actions.WriteCounter(counterDetail);

                    result.Counters.LastEtag = counterDetail.Etag;
                }
            }

            return result.Counters;
        }

        private SmugglerProgressBase.Counts ProcessLegacyAttachments(SmugglerResult result)
        {
            using (var actions = _destination.Documents())
            {
                foreach (var item in _source.GetLegacyAttachments(actions))
                {
                    _token.ThrowIfCancellationRequested();

                    result.Documents.ReadCount++;
                    result.Documents.Attachments.ReadCount++;
                    if (result.Documents.Attachments.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Documents.Attachments.ReadCount:#,#;;0} legacy attachments.");

                    if (item.Document.Id == null)
                        ThrowInvalidData();

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    actions.WriteDocument(item, result.Documents);

                    result.Documents.LastEtag = item.Document.Etag;
                }
            }

            return result.Documents;
        }

        private SmugglerProgressBase.Counts ProcessLegacyAttachmentDeletions(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            using (var actions = _destination.Documents())
            {
                foreach (var id in _source.GetLegacyAttachmentDeletions())
                {
                    counts.ReadCount++;

                    if (counts.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {counts.ReadCount:#,#;;0} legacy attachment deletions.");

                    try
                    {
                        actions.DeleteDocument(id);
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

        private SmugglerProgressBase.Counts ProcessLegacyDocumentDeletions(SmugglerResult result)
        {
            var counts = new SmugglerProgressBase.Counts();
            using (var actions = _destination.Documents())
            {
                foreach (var id in _source.GetLegacyDocumentDeletions())
                {
                    counts.ReadCount++;

                    if (counts.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {counts.ReadCount:#,#;;0} legacy document deletions.");

                    try
                    {
                        actions.DeleteDocument(id);
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

        private SmugglerProgressBase.Counts ProcessTombstones(SmugglerResult result)
        {
            using (var actions = _destination.Tombstones())
            {
                foreach (var tombstone in _source.GetTombstones(_options.Collections, actions))
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

                    tombstone.Flags = tombstone.Flags.Strip(DocumentFlags.FromClusterTransaction);
                    actions.WriteTombstone(tombstone, result.Tombstones);

                    result.Tombstones.LastEtag = tombstone.Etag;
                }
            }

            return result.Tombstones;
        }

        private SmugglerProgressBase.Counts ProcessConflicts(SmugglerResult result)
        {
            using (var actions = _destination.Conflicts())
            {
                foreach (var conflict in _source.GetConflicts(_options.Collections, actions))
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

                    actions.WriteConflict(conflict, result.Conflicts);

                    result.Conflicts.LastEtag = conflict.Etag;
                }
            }

            return result.Conflicts;
        }

        private static void SkipDocument(DocumentItem item, SmugglerResult result)
        {
            result.Documents.SkippedCount++;

            if (item.Document != null)
            {
                item.Document.Data.Dispose();

                if (item.Attachments != null)
                {
                    foreach (var attachment in item.Attachments)
                    {
                        attachment.Dispose();
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CanSkipDocument(Document document, BuildVersionType buildType)
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

        private static void ThrowInvalidData()
        {
            throw new InvalidDataException("Document does not contain an id.");
        }
    }
}
