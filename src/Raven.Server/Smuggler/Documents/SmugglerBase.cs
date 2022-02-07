using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public abstract class SmugglerBase
    {
        internal readonly ISmugglerSource _source;
        internal readonly DatabaseSmugglerOptionsServerSide _options;
        internal readonly SmugglerResult _result;
        internal readonly SystemTime _time;
        internal readonly Action<IOperationProgress> _onProgress;
        internal readonly CancellationToken _token;
        public readonly JsonOperationContext _context;
        public Action<DatabaseRecord> OnDatabaseRecordAction;

        public SmugglerBase(ISmugglerSource source, SystemTime time, JsonOperationContext context, DatabaseSmugglerOptionsServerSide options = null, 
            SmugglerResult result = null, Action<IOperationProgress> onProgress = null,
            CancellationToken token = default)
        {
            _source = source;
            _options = options ?? new DatabaseSmugglerOptionsServerSide();
            _result = result;
            _token = token;
            _context = context;
            _time = time;
            _onProgress = onProgress ?? (progress => { });
        }

        public abstract Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true);

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
            EnsureStepProcessed(result.ReplicationHubCertificates, skipped);

            static void EnsureStepProcessed(SmugglerProgressBase.Counts counts, bool skipped)
            {
                if (counts.Processed)
                    return;

                counts.Processed = true;
                counts.Skipped = skipped;
            }
        }

        internal async Task ProcessTypeAsync(DatabaseItemType type, SmugglerResult result, BuildVersionType buildType, bool ensureStepsProcessed = true)
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

            result.AddInfo($"Started processing {type}.");
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
                    counts = await ProcessCompareExchangeAsync(result);
                    break;
#pragma warning disable 618
                case DatabaseItemType.Counters:
#pragma warning restore 618
                    counts = await ProcessLegacyCountersAsync(result);
                    break;

                case DatabaseItemType.CounterGroups:
                    counts = await ProcessCountersAsync(result);
                    break;

                case DatabaseItemType.CompareExchangeTombstones:
                    counts = await ProcessCompareExchangeTombstonesAsync(result);
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

                case DatabaseItemType.ReplicationHubCertificates:
                    counts = result.ReplicationHubCertificates;
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

        protected abstract Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result);

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

                try
                {
                    await actions.WriteDatabaseRecordAsync(databaseRecord, result.DatabaseRecord, _options.AuthorizationStatus, _options.OperateOnDatabaseRecordTypes);
                }
                catch (Exception e)
                {
                    result.AddError($"Could not write database record: {e.Message}");
                    result.DatabaseRecord.ErroredCount++;
                }
            }

            return result.DatabaseRecord;
        }

        protected abstract Task<SmugglerProgressBase.Counts> ProcessDocumentsAsync(SmugglerResult result, BuildVersionType buildType);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessRevisionDocumentsAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessTombstonesAsync(SmugglerResult result, BuildVersionType buildType);

        protected abstract Task ProcessDocumentsWithDuplicateCollectionAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessConflictsAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessIdentitiesAsync(SmugglerResult result, BuildVersionType buildType);

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

        protected abstract Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentsAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessLegacyDocumentDeletionsAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentDeletionsAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessLegacyCountersAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessCountersAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result);

        protected abstract Task<SmugglerProgressBase.Counts> ProcessSubscriptionsAsync(SmugglerResult result);

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

        protected abstract Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result);

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

        protected abstract Task<SmugglerProgressBase.Counts> ProcessTimeSeriesAsync(SmugglerResult result);
        
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

        protected static void SkipDocument(DocumentItem item, SmugglerResult result)
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

        protected static void ThrowInvalidData()
        {
            throw new InvalidDataException("Document does not contain an id.");
        }
    }
}
