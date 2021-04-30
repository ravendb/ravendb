using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
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

            Debug.Assert((source is DatabaseSource && destination is DatabaseDestination) == false,
                "When both source and destination are database, we might get into a delayed write for the dest while the " +
                "source already pulsed its' read transaction, resulting in bad memory read.");

            _time = time;
            _onProgress = onProgress ?? (progress => { });
        }

        /// <summary>
        /// isLastFile param true by default to correctly restore identities and compare exchange from V41 ravendbdump file.
        /// </summary>
        /// <param name="ensureStepsProcessed"></param>
        /// <param name="isLastFile"></param>
        /// <returns></returns>
        public SmugglerResult Execute(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            var result = _result ?? new SmugglerResult();

            using (_patcher?.Initialize())
            using (_source.Initialize(_options, result, out long buildVersion))
            using (_destination.Initialize(_options, result, buildVersion))
            {
                ModifyV41OperateOnTypes(buildVersion, isLastFile);

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

        private void ModifyV41OperateOnTypes(long buildVersion, bool isLastFile)
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

        public static void EnsureProcessed(SmugglerResult result, bool skipped = true)
        {
            EnsureStepProcessed(result.DatabaseRecord, skipped);
            EnsureStepProcessed(result.Documents, skipped);
            EnsureStepProcessed(result.Documents.Attachments, skipped);
            EnsureStepProcessed(result.RevisionDocuments, skipped);
            EnsureStepProcessed(result.RevisionDocuments.Attachments, skipped);
            EnsureStepProcessed(result.Counters, skipped);
            EnsureStepProcessed(result.Tombstones, skipped);
            EnsureStepProcessed(result.Conflicts, skipped);
            EnsureStepProcessed(result.Indexes, skipped);
            EnsureStepProcessed(result.Identities, skipped);
            EnsureStepProcessed(result.CompareExchange, skipped);
            EnsureStepProcessed(result.CompareExchangeTombstones, skipped);
            EnsureStepProcessed(result.Subscriptions, skipped);
            EnsureStepProcessed(result.TimeSeries, skipped);

            static void EnsureStepProcessed(SmugglerProgressBase.Counts counts, bool skipped)
            {
                if (counts.Processed)
                    return;

                counts.Processed = true;
                counts.Skipped = skipped;
            }
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
                    counts = ProcessTombstones(result, buildType);
                    ProcessDocumentsWithDuplicateCollection(result);
                    break;
                case DatabaseItemType.Conflicts:
                    counts = ProcessConflicts(result);
                    break;
                case DatabaseItemType.Indexes:
                    counts = ProcessIndexes(result);
                    break;
                case DatabaseItemType.Identities:
                    counts = ProcessIdentities(result, buildType);
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
#pragma warning disable 618
                case DatabaseItemType.Counters:
#pragma warning restore 618
                    counts = ProcessLegacyCounters(result);
                    break;
                case DatabaseItemType.CounterGroups:
                    counts = ProcessCounters(result);
                    break;
                case DatabaseItemType.CompareExchangeTombstones:
                    counts = ProcessCompareExchangeTombstones(result);
                    break;
                case DatabaseItemType.Subscriptions:
                    counts = ProcessSubscriptions(result);
                    break;
                case DatabaseItemType.TimeSeries:
                    counts = ProcessTimeSeries(result);
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

            result.AddInfo($"Finished processing {type}. {counts}");
            _onProgress.Invoke(result.Progress);
        }

        private void ProcessDocumentsWithDuplicateCollection(SmugglerResult result)
        {
            var didWork = false;
            var count = 0;
            using (var actions = _destination.Documents())
            {
                foreach (var item in actions.GetDocumentsWithDuplicateCollection())
                {
                    if (didWork == false)
                    {
                        result.AddInfo("Starting to process documents with duplicate collection.");
                        didWork = true;
                    }
                    actions.WriteDocument(item, result.Documents);
                    count++;
                }
            }

            if (didWork)
                result.AddInfo($"Finished processing '{count}' documents with duplicate collection.");
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
#pragma warning disable 618
                case DatabaseItemType.Counters:
#pragma warning restore 618
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

        private SmugglerProgressBase.Counts ProcessIdentities(SmugglerResult result, BuildVersionType buildType)
        {
            result.Identities.Start();

            using (var actions = _destination.Identities())
            {
                foreach (var identity in _source.GetIdentities())
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

                        actions.WriteKeyValue(identityPrefix, identity.Value);
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

        private SmugglerProgressBase.Counts ProcessIndexes(SmugglerResult result)
        {
            result.Indexes.Start();

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

                    if (result.Indexes.ReadCount % 10 == 0)
                    {
                        var message = $"Read {result.Indexes.ReadCount:#,#;;0} indexes.";
                        AddInfoToSmugglerResult(result, message);
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
            result.DatabaseRecord.Start();

            using (var actions = _destination.DatabaseRecord())
            {
                var databaseRecord = _source.GetDatabaseRecord();

                _token.ThrowIfCancellationRequested();

                if (OnDatabaseRecordAction != null)
                {
                    OnDatabaseRecordAction(databaseRecord);
                    return new SmugglerProgressBase.DatabaseRecordProgress();
                }

                result.DatabaseRecord.ReadCount++;

                try
                {
                    actions.WriteDatabaseRecord(databaseRecord, result.DatabaseRecord, _options.AuthorizationStatus, _options.OperateOnDatabaseRecordTypes);
                }
                catch (Exception e)
                {
                    result.AddError($"Could not write database record: {e.Message}");
                    result.DatabaseRecord.ErroredCount++;
                }
            }

            return result.DatabaseRecord;
        }

        private SmugglerProgressBase.Counts ProcessRevisionDocuments(SmugglerResult result)
        {
            result.RevisionDocuments.Start();

            using (var actions = _destination.RevisionDocuments())
            {
                foreach (var item in _source.GetRevisionDocuments(_options.Collections, actions))
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

                    actions.WriteDocument(item, result.RevisionDocuments);

                    result.RevisionDocuments.LastEtag = item.Document.Etag;
                }
            }

            return result.RevisionDocuments;
        }

        private SmugglerProgressBase.Counts ProcessDocuments(SmugglerResult result, BuildVersionType buildType)
        {
            result.Documents.Start();

            var throwOnCollectionMismatchError = _options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false;
            using (var actions = _destination.Documents(throwOnCollectionMismatchError))
            {
                List<LazyStringValue> legacyIdsToDelete = null;
                foreach (var item in _source.GetDocuments(_options.Collections, actions))
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
                        item.Document = _patcher.Transform(item.Document, actions.GetContextForNewDocument());
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

                    actions.WriteDocument(item, result.Documents);
                }

                TryHandleLegacyDocumentTombstones(legacyIdsToDelete, actions, result);
            }

            if (buildType == BuildVersionType.V3 && result.RevisionDocuments.ReadCount > 0)
                result.RevisionDocuments.Processed = true;

            return result.Documents;
        }

        private void TryHandleLegacyDocumentTombstones(List<LazyStringValue> legacyIdsToDelete, IDocumentActions actions, SmugglerResult result)
        {
            if (legacyIdsToDelete == null)
                return;

            foreach (var idToDelete in legacyIdsToDelete)
            {
                actions.DeleteDocument(idToDelete);

                result.Tombstones.ReadCount++;
                if (result.Tombstones.ReadCount % 1000 == 0)
                    AddInfoToSmugglerResult(result, $"Read {result.Tombstones.ReadCount:#,#;;0} tombstones.");
            }
        }

        private void AddInfoToSmugglerResult(SmugglerResult result, string message)
        {
            result.AddInfo(message);
            _onProgress.Invoke(result.Progress);
        }

        private void SetDocumentOrTombstoneFlags(ref DocumentFlags flags, ref NonPersistentDocumentFlags nonPersistentFlags, BuildVersionType buildType)
        {
            flags = flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.FromReplication);
            nonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

            if (_options.SkipRevisionCreation)
                nonPersistentFlags |= NonPersistentDocumentFlags.SkipRevisionCreationForSmuggler;

            switch (buildType)
            {
                case BuildVersionType.V4:
                case BuildVersionType.V5:
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

        private bool SkipDocument(BuildVersionType buildType, bool isPreV4Revision, DocumentItem item, SmugglerResult result, ref List<LazyStringValue> legacyIdsToDelete)
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

        private SmugglerProgressBase.Counts ProcessCompareExchange(SmugglerResult result)
        {
            result.CompareExchange.Start();

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
                        actions.WriteKeyValue(kvp.Key.Key, kvp.Value);
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

        private SmugglerProgressBase.Counts ProcessCounters(SmugglerResult result)
        {
            result.Counters.Start();

            using (var actions = _destination.Counters(result))
            {
                foreach (var counterGroup in _source.GetCounterValues(_options.Collections, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.Counters.ReadCount++;

                    if (result.Counters.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Counters.ReadCount:#,#;;0} counters.");

                    actions.WriteCounter(counterGroup);

                    result.Counters.LastEtag = counterGroup.Etag;
                }
            }

            return result.Counters;
        }

        private SmugglerProgressBase.Counts ProcessLegacyCounters(SmugglerResult result)
        {
            using (var actions = _destination.Counters(result))
            {
                foreach (var counterDetail in _source.GetLegacyCounterValues())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Counters.ReadCount++;

                    if (result.Counters.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Counters.ReadCount:#,#;;0} counters.");

                    actions.WriteLegacyCounter(counterDetail);

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

        private SmugglerProgressBase.Counts ProcessTombstones(SmugglerResult result, BuildVersionType buildType)
        {
            result.Tombstones.Start();

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


                    if (_options.IncludeArtificial == false && tombstone.Flags.HasFlag(DocumentFlags.Artificial))
                    {
                        continue;
                    }

                    var _ = NonPersistentDocumentFlags.None;
                    SetDocumentOrTombstoneFlags(ref tombstone.Flags, ref _, buildType);

                    actions.WriteTombstone(tombstone, result.Tombstones);

                    result.Tombstones.LastEtag = tombstone.Etag;
                }
            }

            return result.Tombstones;
        }

        private SmugglerProgressBase.Counts ProcessCompareExchangeTombstones(SmugglerResult result)
        {
            result.CompareExchangeTombstones.Start();

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var actions = _destination.CompareExchangeTombstones(context))
            {
                foreach (var key in _source.GetCompareExchangeTombstones())
                {
                    _token.ThrowIfCancellationRequested();
                    result.CompareExchangeTombstones.ReadCount++;

                    if (key.Equals(default))
                    {
                        result.CompareExchangeTombstones.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        actions.WriteTombstoneKey(key.Key.Key);
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

        private SmugglerProgressBase.Counts ProcessConflicts(SmugglerResult result)
        {
            result.Conflicts.Start();

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

        private SmugglerProgressBase.Counts ProcessSubscriptions(SmugglerResult result)
        {
            result.Subscriptions.Start();

            using (var actions = _destination.Subscriptions())
            {
                foreach (var subscription in _source.GetSubscriptions())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Subscriptions.ReadCount++;

                    if (result.Subscriptions.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.Subscriptions.ReadCount:#,#;;0} subscription.");

                    actions.WriteSubscription(subscription);
                }
            }

            return result.Subscriptions;
        }

        private SmugglerProgressBase.Counts ProcessTimeSeries(SmugglerResult result)
        {
            result.TimeSeries.Start();

            using (var actions = _destination.TimeSeries())
            {
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                foreach (var ts in _source.GetTimeSeries(_options.Collections))
                {
                    _token.ThrowIfCancellationRequested();
                    result.TimeSeries.ReadCount += ts.Segment.NumberOfEntries;

                    if (result.TimeSeries.ReadCount % 1000 == 0)
                        AddInfoToSmugglerResult(result, $"Read {result.TimeSeries.ReadCount:#,#;;0} time series.");

                    result.TimeSeries.LastEtag = ts.Etag;

                    var shouldSkip = isFullBackup && ts.Segment.NumberOfLiveEntries == 0;
                    if (shouldSkip == false)
                        actions.WriteTimeSeries(ts);
                }
            }

            return result.TimeSeries;
        }

        private static void SkipDocument(DocumentItem item, SmugglerResult result)
        {
            result.Documents.SkippedCount++;

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
