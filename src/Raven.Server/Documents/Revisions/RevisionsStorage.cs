using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Types;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Utils.MetricCacher.Keys;
using static Voron.Data.Tables.Table;
using Constants = Raven.Client.Constants;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        public readonly TableSchema RevisionsSchema;
        public readonly TableSchema CompressedRevisionsSchema;
        public long SizeLimitInBytes = new Size(PlatformDetails.Is32Bits == false ? 32 : 2, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);

        public RevisionsConfiguration ConflictConfiguration;
        public const long NotDeletedRevisionMarker = 0;
        public readonly RevisionsOperations Operations;

        public RevisionsConfiguration Configuration { get; private set; }

        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Logger _logger;
        private static readonly TimeSpan MaxEnforceConfigurationSingleBatchTime = TimeSpan.FromSeconds(30);
        private readonly RevisionsCollectionConfiguration _emptyConfiguration = new RevisionsCollectionConfiguration { Disabled = true };

        public RevisionsStorage([NotNull] DocumentDatabase database, [NotNull] Transaction tx, [NotNull] TableSchema revisionsSchema, [NotNull] TableSchema compressedRevisionsSchema)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));

            _database = database ?? throw new ArgumentNullException(nameof(database));
            _documentsStorage = _database.DocumentsStorage;

            RevisionsSchema = revisionsSchema ?? throw new ArgumentNullException(nameof(revisionsSchema));
            CompressedRevisionsSchema = compressedRevisionsSchema ?? throw new ArgumentNullException(nameof(compressedRevisionsSchema));

            _logger = LoggingSource.Instance.GetLogger<RevisionsStorage>(database.Name);
            Operations = new RevisionsOperations(_database);
            ConflictConfiguration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    MinimumRevisionsToKeep = 1024,
                    MaximumRevisionsToDeleteUponDocumentUpdate = 10 * 1024,
                    Disabled = false
                }
            };
            CreateTrees(tx);
        }

        public Table EnsureRevisionTableCreated(Transaction tx, CollectionName collection)
        {
            var revisionsSchema = _database.DocumentsCompression.CompressRevisions ?
                CompressedRevisionsSchema :
                RevisionsSchema;

            return EnsureRevisionTableCreated(tx, collection, revisionsSchema);
        }

        internal Table EnsureRevisionTableCreated(Transaction tx, CollectionName collection, TableSchema schema)
        {
            var tableName = collection.GetTableName(CollectionTableType.Revisions);

            if (_tableCreated.Contains(collection.Name) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                schema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                    {
                        collection.Name
                    };
                };
            }

            return tx.OpenTable(schema, tableName);
        }

        public void InitializeFromDatabaseRecord(DatabaseRecord dbRecord)
        {
            try
            {
                if (dbRecord.RevisionsForConflicts != null)
                    ConflictConfiguration.Default = dbRecord.RevisionsForConflicts;

                var revisions = dbRecord.Revisions;
                if (revisions == null || (revisions.Default == null && revisions.Collections.Count == 0))
                {
                    Configuration = null;
                    return;
                }

                if (revisions.Equals(Configuration))
                    return;

                Configuration = revisions;

                if (_logger.IsInfoEnabled)
                    _logger.Info("Revisions configuration changed");
            }
            catch (Exception e)
            {
                const string message = "Failed to enable revisions for documents as the revisions configuration " +
                          "in the database record is missing or not valid.";

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    $"Revisions error in {_database.Name}", message,
                    AlertType.RevisionsConfigurationNotValid,
                    NotificationSeverity.Error,
                    _database.Name,
                    details: new ExceptionDetails(e)));

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);
            }
        }

        private void CreateTrees(Transaction tx)
        {
            tx.CreateTree(RevisionsCountSlice);
            _documentsStorage.TombstonesSchema.Create(tx, RevisionsTombstonesSlice, 16);
        }

        public RevisionsCollectionConfiguration GetRevisionsConfiguration(string collection, DocumentFlags flags = DocumentFlags.None, bool deleteRevisionsWhenNoCofiguration = false)
        {
            if (Configuration != null)
            {
                if (Configuration.Collections != null &&
                    Configuration.Collections.TryGetValue(collection, out RevisionsCollectionConfiguration configuration))
                    return configuration;

                if (Configuration.Default != null)
                    return Configuration.Default;
            }

            if (flags.Contain(DocumentFlags.Resolved) || flags.Contain(DocumentFlags.Conflicted))
            {
                return ConflictConfiguration.Default;
            }

            return deleteRevisionsWhenNoCofiguration ? ZeroConfiguration : _emptyConfiguration;
        }

        public bool ShouldVersionDocument(CollectionName collectionName, NonPersistentDocumentFlags nonPersistentFlags,
            BlittableJsonReaderObject existingDocument, BlittableJsonReaderObject document,
            DocumentsOperationContext context, string id,
            long? lastModifiedTicks,
            ref DocumentFlags documentFlags, out RevisionsCollectionConfiguration docConfiguration)
        {
            docConfiguration = GetRevisionsConfiguration(collectionName.Name, documentFlags);

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return false;

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.SkipRevisionCreation))
                return false;

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromSmuggler))
            {
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByCountersUpdate))
                    return false;

                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByAttachmentUpdate))
                    return false;

                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByTimeSeriesUpdate))
                    return false;

                if (docConfiguration == ConflictConfiguration.Default || docConfiguration == _emptyConfiguration || docConfiguration.Disabled)
                    return false;
            }

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.Resolved))
                return true;

            if (docConfiguration == ConflictConfiguration.Default || docConfiguration == _emptyConfiguration)
            {
                // If comes from resolver (creating conflicted/resolved revision when resolving a conflict), and doc has no config, do not touch the revisions.
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResolver))
                    return false;

                if (documentFlags.Contain(DocumentFlags.HasRevisions) == false) // If the doc has revisions but no config, do not touch the revisions
                    return false;
            }

            if (docConfiguration.Disabled)
                return false;

            if (docConfiguration.MinimumRevisionsToKeep == 0)
                return true;

            if (docConfiguration.MinimumRevisionAgeToKeep.HasValue && lastModifiedTicks.HasValue)
                return true;

            if (existingDocument == null)
            {
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.SkipRevisionCreationForSmuggler))
                {
                    // Smuggler is configured to avoid creating new revisions during import
                    return false;
                }

                // we are not going to create a revision if it's an import from v3
                // (since this import is going to import revisions as well)
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.LegacyHasRevisions))
                {
                    documentFlags |= DocumentFlags.HasRevisions;
                    return false;
                }

                return true;
            }

            if (documentFlags.Contain(DocumentFlags.Reverted))
                return true; // we always want to create a new version for a reverted document

            // compare the contents of the existing and the new document
            if (DocumentCompare.IsEqualTo(existingDocument, document, DocumentCompare.DocumentCompareOptions.Default) != DocumentCompareResult.NotEqual)
            {
                // no need to create a new revision, both documents have identical content
                return false;
            }

            return true;
        }

        public bool ShouldVersionOldDocument(DocumentsOperationContext context, DocumentFlags flags, BlittableJsonReaderObject oldDoc, ChangeVector changeVector, CollectionName collectionName)
        {
            if (oldDoc == null)
                return false; // no document to version

            if (flags.Contain(DocumentFlags.HasRevisions))
                return false; // version already exists

            if (flags.Contain(DocumentFlags.Resolved))
            {
                if (Configuration == null)
                    return false;
                var configuration = GetRevisionsConfiguration(collectionName.Name);

                if (configuration.Disabled)
                    return false;

                if (configuration.MinimumRevisionsToKeep == 0)
                    return false;

                using (Slice.From(context.Allocator, changeVector.Version, out Slice changeVectorSlice))
                {
                    var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                    // True if we already versioned it with the a conflicted flag
                    // False if we didn't resolved the conflict locally

                    return (table.ReadByKey(changeVectorSlice, out var tvr) == false);
                }
            }

            return true;
        }

        public unsafe bool Put(DocumentsOperationContext context, string id, BlittableJsonReaderObject document,
            DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, ChangeVector changeVector, long lastModifiedTicks,
            RevisionsCollectionConfiguration configuration = null, CollectionName collectionName = null)
        {
            Debug.Assert(changeVector != null, "Change vector must be set");
            Debug.Assert(lastModifiedTicks != DateTime.MinValue.Ticks, "last modified ticks must be set");

            BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);

            if (collectionName == null)
                collectionName = _database.DocumentsStorage.ExtractCollectionName(context, document);
            if (configuration == null)
                configuration = GetRevisionsConfiguration(collectionName.Name, flags);

            if (configuration.Disabled &&
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false &&
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.ForceRevisionCreation) == false &&
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromSmuggler) == false)
                return false;

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idSlice))
            using (Slice.From(context.Allocator, changeVector.Version, out Slice changeVectorSlice))
            {
                var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                var revisionExists = table.ReadByKey(changeVectorSlice, out var tvr);

                if (revisionExists)
                {
                    MarkRevisionsAsConflictedIfNeeded(context, lowerId, idSlice, flags, tvr, table, changeVectorSlice);
                    return false;
                }

                // We want the revision's attachments to have a lower etag than the revision itself
                if (flags.Contain(DocumentFlags.HasAttachments) &&
                    flags.Contain(DocumentFlags.Revision) == false)
                {
                    _documentsStorage.AttachmentsStorage.RevisionAttachments(context, document, lowerId, changeVectorSlice);
                }

                PutFromRevisionIfChangeVectorIsGreater(context, document, id, changeVector, lastModifiedTicks, flags, nonPersistentFlags);

                if (table.VerifyKeyExists(changeVectorSlice)) // we might create
                    return true;

                using var revision = AddCounterAndTimeSeriesSnapshotsIfNeeded(context, id, document.Clone(context));

                flags |= DocumentFlags.Revision;
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ForceRevisionCreation))
                    flags |= DocumentFlags.ForceCreated;

                var etag = _database.DocumentsStorage.GenerateNextEtag();
                var newEtagSwapBytes = Bits.SwapBytes(etag);

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);
                    tvb.Add(lowerId);
                    tvb.Add(SpecialChars.RecordSeparator);
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(idSlice);
                    tvb.Add(revision.BasePointer, revision.Size);
                    tvb.Add((int)flags);
                    tvb.Add(NotDeletedRevisionMarker);
                    tvb.Add(lastModifiedTicks);
                    tvb.Add(context.GetTransactionMarker());
                    if (flags.Contain(DocumentFlags.Resolved))
                    {
                        tvb.Add((int)DocumentFlags.Resolved);
                    }
                    else
                    {
                        tvb.Add(0);
                    }
                    tvb.Add(Bits.SwapBytes(lastModifiedTicks));
                    table.Insert(tvb);
                }

                using (GetKeyPrefix(context, lowerId, out Slice lowerIdPrefix))
                {
                    IncrementCountOfRevisions(context, lowerIdPrefix, 1);
                    DeleteOldRevisions(context, table, lowerIdPrefix, collectionName, configuration, nonPersistentFlags, changeVector, lastModifiedTicks, documentDeleted: false, skipForceCreated: false);
                }
            }

            return true;
        }

        private BlittableJsonReaderObject AddCounterAndTimeSeriesSnapshotsIfNeeded(DocumentsOperationContext context, string id, BlittableJsonReaderObject document)
        {
            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                return document;

            if (metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counterNames))
            {
                var djv = new DynamicJsonValue();
                for (var i = 0; i < counterNames.Length; i++)
                {
                    var counter = counterNames[i].ToString();
                    var val = _documentsStorage.CountersStorage.GetCounterValue(context, id, counter, capOnOverflow: true)?.Value;
                    if (val == null)
                        continue;
                    djv[counter] = val.Value;
                }

                metadata.Modifications = new DynamicJsonValue(metadata)
                {
                    [Constants.Documents.Metadata.RevisionCounters] = djv
                };

                metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
            }

            if (metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesNames))
            {
                var djv = new DynamicJsonValue();
                for (var i = 0; i < timeSeriesNames.Length; i++)
                {
                    var name = timeSeriesNames[i].ToString();
                    var (count, start, end) = _documentsStorage.TimeSeriesStorage.Stats.GetStats(context, id, name);
                    Debug.Assert(start == default || start.Kind == DateTimeKind.Utc);

                    djv[name] = new DynamicJsonValue
                    {
                        ["Count"] = count,
                        ["Start"] = start,
                        ["End"] = end
                    };
                }

                metadata.Modifications ??= new DynamicJsonValue(metadata);

                metadata.Modifications[Constants.Documents.Metadata.RevisionTimeSeries] = djv;

                metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);

            }

            if (metadata.Modifications != null)
            {
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };

                using (var old = document)
                {
                    return context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }
            }

            return document;
        }

        private void PutFromRevisionIfChangeVectorIsGreater(
            DocumentsOperationContext context,
            BlittableJsonReaderObject document,
            string id,
            ChangeVector changeVector,
            long lastModifiedTicks,
            DocumentFlags flags,
            NonPersistentDocumentFlags nonPersistentFlags,
            CollectionName collectionName = null)
        {
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false)
                return;

            if ((flags.Contain(DocumentFlags.Revision) || flags.Contain(DocumentFlags.DeleteRevision)) == false)
                return; // only revision can overwrite the document

            if (flags.Contain(DocumentFlags.Conflicted))
                return; // but, conflicted revision can't

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out var lowerId, out _))
            {
                var conflictStatus = ConflictsStorage.GetConflictStatusForDocument(context, id, changeVector, out _);
                if (conflictStatus != ConflictStatus.Update)
                    return; // Do not modify the document.

                if (flags.Contain(DocumentFlags.Resolved))
                {
                    _database.ReplicationLoader.ConflictResolver.SaveLocalAsRevision(context, id);
                }

                nonPersistentFlags |= NonPersistentDocumentFlags.SkipRevisionCreation;
                flags = flags.Strip(DocumentFlags.Revision | DocumentFlags.DeleteRevision) | DocumentFlags.HasRevisions;

                if (document == null)
                {
                    _documentsStorage.Delete(context, lowerId, id, null, lastModifiedTicks, changeVector, collectionName,
                        nonPersistentFlags, flags);
                    return;
                }

                using var reverted = RevertSnapshotFlags(context, document.CloneOnTheSameContext(), id);
                _documentsStorage.Put(context, id, null, reverted, lastModifiedTicks, changeVector,
                    null, flags, nonPersistentFlags);
            }
        }

        private static bool RevertSnapshotFlag(BlittableJsonReaderObject metadata, string snapshotFlag, string flag)
        {
            if (metadata.TryGet(snapshotFlag, out BlittableJsonReaderObject bjro) == false)
                return false;

            var names = bjro.GetPropertyNames();

            metadata.Modifications ??= new DynamicJsonValue(metadata);
            metadata.Modifications.Remove(snapshotFlag);
            var arr = new DynamicJsonArray();
            foreach (var name in names)
            {
                arr.Add(name);
            }

            metadata.Modifications[flag] = arr;

            return true;
        }

        private static BlittableJsonReaderObject RevertSnapshotFlags(DocumentsOperationContext context, BlittableJsonReaderObject document, string documentId)
        {
            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                return document;

            var metadataModified = RevertSnapshotFlag(metadata, Constants.Documents.Metadata.RevisionCounters, Constants.Documents.Metadata.Counters);
            metadataModified |= RevertSnapshotFlag(metadata, Constants.Documents.Metadata.RevisionTimeSeries, Constants.Documents.Metadata.TimeSeries);

            if (metadataModified)
            {
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };

                using (var old = document)
                    document = context.ReadObject(document, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }

            return document;
        }

        public class DeleteOldRevisionsResult
        {
            public bool HasMore;
            public long PreviousCount;
            public long Remaining;
            public int Skip;
        }

        private DeleteOldRevisionsResult DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice lowerIdPrefix, CollectionName collectionName,
            RevisionsCollectionConfiguration configuration, NonPersistentDocumentFlags nonPersistentFlags, ChangeVector changeVector, long lastModifiedTicks,
            bool documentDeleted, bool skipForceCreated, DocumentFlags flags = DocumentFlags.None)
        {
            var result = new DeleteOldRevisionsResult();
            result.PreviousCount = GetRevisionsCount(context, lowerIdPrefix);

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return result;


            IEnumerable<Document> revisionsToDelete;
            var conflicted = false;

            if (configuration == ConflictConfiguration.Default
                     || configuration == ZeroConfiguration) // conflict revisions config
            {
                revisionsToDelete = GetRevisionsForConflict(context, table, lowerIdPrefix,
                    nonPersistentFlags, skipForceCreated, result.PreviousCount, documentDeleted, result);

                conflicted = true;
            }
            else if (documentDeleted && configuration.PurgeOnDelete) // doc is deleted or came from delete *and* configuration.PurgeOnDelete is true
            {
                revisionsToDelete = GetAllRevisions(context, table, lowerIdPrefix,
                    maxDeletesUponUpdate: null, skipForceCreated: false, result);
            }
            else
            {
                revisionsToDelete = GetRevisionsForCollectionOrDefault(context, table, lowerIdPrefix,
                    configuration, result.PreviousCount,
                    stopWhenReachingAge: nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByEnforceRevisionConfiguration) == false,
                    result);
            }

            var deleted = DeleteRevisionsInternal(context, table, lowerIdPrefix, collectionName, changeVector, lastModifiedTicks, result.PreviousCount, revisionsToDelete, result, tombstoneFlags: flags);

            IncrementCountOfRevisions(context, lowerIdPrefix, -deleted);
            result.Remaining = result.PreviousCount - deleted;

            if (ShouldAddConflictRevisionNotification(conflicted, nonPersistentFlags, deleted))
            {
                var reason = ConflictRevisionsExceeded.ExceedingReason.MinimumRevisionsToKeep;
                if (ConflictConfiguration.Default.MinimumRevisionAgeToKeep.HasValue)
                    reason = ConflictRevisionsExceeded.ExceedingReason.MinimumRevisionAgeToKeep;

                _database.NotificationCenter.ConflictRevisionsExceeded.Add(new ConflictRevisionsExceeded.ConflictInfo(lowerIdPrefix.ToString(), reason, deleted, _database.Time.GetUtcNow()));
            }

            return result;
        }

        private static bool ShouldAddConflictRevisionNotification(bool conflicted, NonPersistentDocumentFlags nonPersistentFlags, long deleted)
        {
            return conflicted &&
                   nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByEnforceRevisionConfiguration) == false &&
                   nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromSmuggler) == false &&
                   deleted > 0;
        }

        public void DeleteAllRevisionsFor(DocumentsOperationContext context, string id, bool skipForceCreated, ref bool moreWork)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerIdPrefix))
            using (GetKeyPrefix(context, lowerIdPrefix, out Slice prefixSlice))
            {
                var collectionName = GetCollectionFor(context, prefixSlice);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete all revisions for '{id}' but no revisions found.");
                    return;
                }

                var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                var newEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);

                var configuration = GetRevisionsConfiguration(collectionName.Name, deleteRevisionsWhenNoCofiguration: true);

                var maxDeletesUponUpdate = configuration.MaximumRevisionsToDeleteUponDocumentUpdate;

                var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;
                var result = new DeleteOldRevisionsResult();
                var revisionsToDelete = GetAllRevisions(context, table, prefixSlice, maxDeletesUponUpdate, skipForceCreated, result);
                var revisionsPreviousCount = GetRevisionsCount(context, prefixSlice);
                var deleted = DeleteRevisionsInternal(context, table, lowerIdPrefix, collectionName, changeVector, lastModifiedTicks, revisionsPreviousCount, revisionsToDelete, result, tombstoneFlags: DocumentFlags.None);
                moreWork |= result.HasMore;
                IncrementCountOfRevisions(context, prefixSlice, -deleted);
            }
        }

        public long DeleteRevisionsFor(DocumentsOperationContext context, string id, bool fromDelete = false)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice lowerIdPrefix))
            {
                var collectionName = GetCollectionFor(context, lowerIdPrefix);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete all revisions for '{id}' but no revisions found.");
                    return 0;
                }

                var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                var newEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);

                var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;
                var configuration = GetRevisionsConfiguration(collectionName.Name, deleteRevisionsWhenNoCofiguration: true);

                if (fromDelete == false)
                {
                    var local = _documentsStorage.GetDocumentOrTombstone(context, lowerId, throwOnConflict: false);
                    fromDelete = local.Document == null && local.Tombstone != null;
                    Debug.Assert(local.Document != null || local.Tombstone != null);
                }

                var result = DeleteOldRevisions(context, table, lowerIdPrefix, collectionName, configuration,
                    NonPersistentDocumentFlags.None,
                    changeVector, lastModifiedTicks, fromDelete, skipForceCreated: false);

                return result.Remaining;
            }
        }

        public void DeleteRevisionsBefore(DocumentsOperationContext context, string collection, DateTime time)
        {
            var collectionName = new CollectionName(collection);
            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
            table.DeleteByPrimaryKey(Slices.BeforeAllKeys, deleted =>
            {
                var lastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref deleted.Reader);
                if (lastModified >= time)
                    return false;

                // We won't create tombstones here as it might create LOTS of tombstones
                // with the same transaction marker and the same change vector.

                using (TableValueToSlice(context, (int)RevisionsTable.LowerId, ref deleted.Reader, out Slice lowerId))
                using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    IncrementCountOfRevisions(context, prefixSlice, -1);
                }

                return true;
            });
        }

        private unsafe CollectionName GetCollectionFor(DocumentsOperationContext context, Slice prefixSlice)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
            var tvr = table.SeekOneForwardFromPrefix(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice);
            if (tvr == null)
                return null;

            var ptr = tvr.Reader.Read((int)RevisionsTable.Document, out int size);
            var data = new BlittableJsonReaderObject(ptr, size, context);

            return _documentsStorage.ExtractCollectionName(context, data);
        }

        public IEnumerable<string> GetCollections(Transaction transaction)
        {
            using (var it = transaction.LowLevelTransaction.RootObjects.Iterate(false))
            {
                it.SetRequiredPrefix(RevisionsPrefix);

                if (it.Seek(RevisionsPrefix) == false)
                    yield break;

                do
                {
                    var collection = it.CurrentKey.ToString();
                    yield return collection.Substring(RevisionsPrefix.Size);
                }
                while (it.MoveNext());
            }
        }

        private IEnumerable<Document> GetRevisionsForCollectionOrDefault(
            DocumentsOperationContext context, Table table,
            Slice prefixSlice,
            RevisionsCollectionConfiguration configuration,
            long revisionsCount,
            bool stopWhenReachingAge,
            DeleteOldRevisionsResult result)
        {
            result.HasMore = false;
            var deleted = 0L;

            long numberOfRevisionsToDelete;
            var hasMaxUponUpdate = false;

            if (configuration.MinimumRevisionsToKeep.HasValue == false
                && configuration.MinimumRevisionAgeToKeep.HasValue == false) // doc isn't deleted and there's no limmit in the config except PurgeOnDelete
            {
                yield break;
            }

            if
                (configuration.MinimumRevisionsToKeep.HasValue) // obey the configuration.MinimumRevisionsToKeep (and the configuration.MaximumRevisionsToDeleteUponDocumentUpdate)
            {
                numberOfRevisionsToDelete = revisionsCount - configuration.MinimumRevisionsToKeep.Value;
                if (numberOfRevisionsToDelete > 0 && configuration.MaximumRevisionsToDeleteUponDocumentUpdate.HasValue &&
                    configuration.MaximumRevisionsToDeleteUponDocumentUpdate.Value < numberOfRevisionsToDelete)
                {
                    numberOfRevisionsToDelete = configuration.MaximumRevisionsToDeleteUponDocumentUpdate.Value;
                    hasMaxUponUpdate = true;
                }

                if (numberOfRevisionsToDelete <= 0)
                    yield break;
            }
            else //  obey the configuration.MinimumRevisionAgeToKeep
            {
                hasMaxUponUpdate = configuration.MaximumRevisionsToDeleteUponDocumentUpdate.HasValue;
                // delete all revisions which age has passed
                numberOfRevisionsToDelete = configuration.MaximumRevisionsToDeleteUponDocumentUpdate ?? long.MaxValue;
            }

            while (true)
            {
                var ended = true;
                foreach (var read in table.SeekForwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, result.Skip, startsWith: true))
                {
                    if (numberOfRevisionsToDelete <= deleted)
                        break;

                    var tvr = read.Result.Reader;
                    var revision = TableValueToRevision(context, ref tvr, DocumentFields.ChangeVector | DocumentFields.LowerId);

                    if (configuration.MinimumRevisionAgeToKeep.HasValue &&
                        _database.Time.GetUtcNow() - revision.LastModified <= configuration.MinimumRevisionAgeToKeep.Value)
                    {
                        revision.Dispose();

                        if (stopWhenReachingAge == false)
                        {
                            result.Skip++;
                            ended = false;
                        }

                        break;
                    }

                    yield return revision;

                    deleted++;

                    ended = false;
                    break;
                }

                if (ended)
                    break;
            }

            Debug.Assert(numberOfRevisionsToDelete >= deleted);
            result.HasMore = hasMaxUponUpdate && deleted == numberOfRevisionsToDelete; // we use maxUponUpdate and we are not in the last delete
                                                                                       // (in the last delete we probably deletes the initialRevisionsCount%maxUponUpdate, which is probably less then maxUponUpdate).
        }

        private long DeleteRevisionsInternal(DocumentsOperationContext context, Table table, Slice lowerIdPrefix, CollectionName collectionName,
            ChangeVector changeVector, long lastModifiedTicks, long revisionsPreviousCount,
            IEnumerable<Document> revisionsToRemove,
            DeleteOldRevisionsResult result,
            DocumentFlags tombstoneFlags)
        {
            var writeTables = new Dictionary<string, Table>();
            long maxEtagDeleted = 0;
            var deleted = 0L;

            var first = true;
            Document lastRevisionToDelete = null;

            foreach (var revision in revisionsToRemove)
            {
                if (first)
                {
                    lastRevisionToDelete = revision;
                    first = false;
                    result.Skip++;
                    continue;
                }

                maxEtagDeleted = Math.Max(maxEtagDeleted, lastRevisionToDelete.Etag);
                DeleteRevisionFromTable(context, table, writeTables, lastRevisionToDelete, collectionName, changeVector, lastModifiedTicks, tombstoneFlags);

                deleted++;
                lastRevisionToDelete = revision;
            }

            // If the last revision you got to remove is the last (newest) revision of the document and it is 'Delete Revision',
            // and the doc still has revisions in addition to it,
            // then don't delete it, so the previous revisions that remained wont become orphan.
            if (lastRevisionToDelete != null)
            {
                var remained = revisionsPreviousCount - deleted;
                var skipLast = lastRevisionToDelete.Flags.Contain(DocumentFlags.DeleteRevision) && remained > 1 &&
                               RevisionIsLast(context, table, lowerIdPrefix, lastRevisionToDelete.Etag);

                if (skipLast == false)
                {
                    maxEtagDeleted = Math.Max(maxEtagDeleted, lastRevisionToDelete.Etag);
                    DeleteRevisionFromTable(context, table, writeTables, lastRevisionToDelete, collectionName, changeVector, lastModifiedTicks, tombstoneFlags);
                    deleted++;
                }
            }

            _database.DocumentsStorage.EnsureLastEtagIsPersisted(context, maxEtagDeleted);
            return deleted;
        }

        private bool RevisionIsLast(DocumentsOperationContext context, Table table, Slice lowerIdPrefix, long etag)
        {
            var loweId = new Slice(context.Allocator.Slice(lowerIdPrefix.Content, 0, lowerIdPrefix.Size - 1)); // cut the prefix seperator from the end of the slice
            using (GetKeyWithEtag(context, loweId, etag, out var compoundPrefix))
            {
                foreach (var read in table.SeekForwardFromPrefix(RevisionsSchema.Indexes[IdAndEtagSlice], start: compoundPrefix, prefix: lowerIdPrefix, skip: 1))
                {
                    return false;
                }
            }

            return true;
        }


        private bool ShouldAdoptRevision(DocumentsOperationContext context, Slice lowerId, Slice lowerIdPrefix, CollectionName collectionName, out Table table, out Document lastRevision)
        {
            lastRevision = null;
            table = null;

            var local = _documentsStorage.Get(context, lowerId, fields: DocumentFields.Default, throwOnConflict: false);
            if (local != null) // doc isn't deleted, so we don't need to create delete revision
                return false;

            lastRevision = GetLastRevisionFor(context, lowerId, lowerIdPrefix, collectionName, out table);
            return lastRevision != null && lastRevision.Flags.Contain(DocumentFlags.DeleteRevision) == false;
        }

        private Document GetLastRevisionFor(DocumentsOperationContext context,
            Slice lowerId,
            Slice lowerIdPrefix,
            CollectionName collectionName,
            out Table table)
        {
            table = null;

            using (GetKeyWithEtag(context, lowerId, etag: long.MaxValue, out var compoundPrefix))
            {
                table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                var holder = table.SeekOneBackwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], lowerIdPrefix, compoundPrefix);
                if (holder == null)
                {
                    table = null;
                    return null;
                }

                return TableValueToRevision(context, ref holder.Reader, DocumentFields.ChangeVector);
            }
        }

        internal void DeleteRevisionFromTable(DocumentsOperationContext context, Table table, Dictionary<string, Table> writeTables,
            Document revision, CollectionName collectionName,
            ChangeVector changeVector, long lastModifiedTicks, DocumentFlags flags)
        {
            using (DocumentIdWorker.GetSliceFromId(context, revision.LowerId, out var prefixSlice))
            using (CreateRevisionTombstoneKeySlice(context, prefixSlice, revision.ChangeVector, out var changeVectorSlice, out var keySlice))
            {
                CreateTombstone(context, keySlice, revision.Etag, collectionName, changeVector, lastModifiedTicks, flags);

                if (revision.Flags.Contain(DocumentFlags.HasAttachments))
                {
                    _documentsStorage.AttachmentsStorage.DeleteRevisionAttachments(context, revision, changeVector, lastModifiedTicks, flags);
                }

                Table writeTable = null;
                if (table.ReadByKey(changeVectorSlice, out TableValueReader tvr) && table.IsOwned(tvr.Id))
                {
                    writeTable = table;
                }
                else
                {
                    // We request to delete revision with the wrong collection
                    var revisionData = TableValueToRevision(context, ref tvr, DocumentFields.Data);

                    var collection = _documentsStorage.ExtractCollectionName(context, revisionData.Data);
                    if (writeTables.TryGetValue(collection.Name, out writeTable) == false)
                    {
                        writeTable = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collection);
                        writeTables[collection.Name] = writeTable;
                    }
                }
                writeTable.DeleteByKey(changeVectorSlice);
            }
        }


        private class ConflictedRevisionsDeletionState
        {
            private readonly RevisionsCollectionConfiguration _config; // conflict revisions config
            private readonly long _conflictCount; // conflict revisions count before the delete
            private readonly long _regularCount;  // not-conflict revisions count before the delete

            private long _regularDeletedCount = 0; // count of not-conflict deleted revisions
            private long _conflictDeletedCount = 0; // count of conflict deleted revisions
            private long _skippedForceCreated = 0;
            public long DeletedCount => _regularDeletedCount + _conflictDeletedCount;
            public bool FinishedRegular { get; private set; }
            public bool FinishedConflicted { get; private set; }

            private readonly bool _skipForceCreated;
            private readonly DateTime _databaseTime;
            private readonly long? _minimumConflictRevisionsToKeep;

            public ConflictedRevisionsDeletionState(long allRevisionCount, long conflictRevisionsCount,
                RevisionsCollectionConfiguration conflictConfig, HandleConflictRevisionsFlags handlingFlags,
                DateTime databaseTime, bool documentDeleted)
            {
                ValidateFlags(handlingFlags);

                _config = conflictConfig;
                _conflictCount = conflictRevisionsCount;
                _regularCount = allRevisionCount - conflictRevisionsCount;

                _databaseTime = databaseTime;
                _skipForceCreated = handlingFlags.HasFlag(HandleConflictRevisionsFlags.ForceCreated) == false;

                FinishedRegular = handlingFlags.HasFlag(HandleConflictRevisionsFlags.Regular) == false || AllRegularAreDeleted();

                if (documentDeleted && _config.PurgeOnDelete)
                {
                    _minimumConflictRevisionsToKeep = 0L;
                    FinishedConflicted = ConflictedReachedMinimumToKeep();
                }
                else if (_config.MinimumRevisionsToKeep.HasValue == false && _config.MinimumRevisionAgeToKeep.HasValue == false)
                {
                    FinishedConflicted = true;
                }
                else
                {
                    _minimumConflictRevisionsToKeep = _config.MinimumRevisionsToKeep;
                    FinishedConflicted = ConflictedReachedMinimumToKeep();
                }
            }

            void ValidateFlags(HandleConflictRevisionsFlags flags)
            {
                if (flags == HandleConflictRevisionsFlags.None)
                {
                    throw new InvalidOperationException($"Cannot delete conflict revisions without deleting.");
                }
                if (flags.HasFlag(HandleConflictRevisionsFlags.Conflicted) == false)
                {
                    throw new InvalidOperationException($"Cannot delete conflict revisions without deleting conflict revisions.");
                }
                if (flags.HasFlag(HandleConflictRevisionsFlags.Regular) == false && flags.HasFlag(HandleConflictRevisionsFlags.ForceCreated))
                {
                    throw new InvalidOperationException($"Cannot delete force-created revisions without deleting also regular revisions.");
                }
            }

            public bool ReachedMaximumRevisionsToDeleteUponDocumentUpdate() =>
                    _config.MaximumRevisionsToDeleteUponDocumentUpdate.HasValue &&
                        _config.MaximumRevisionsToDeleteUponDocumentUpdate.Value <= DeletedCount;

            public bool ShouldDelete(Document revision)
            {
                if (revision.Flags.Contain(DocumentFlags.Conflicted) || revision.Flags.Contain(DocumentFlags.Resolved))
                {
                    if (ShouldDeleteConflicted(revision.LastModified, revision.Flags) == false)
                    {
                        return false;
                    }
                }
                else
                {
                    if (ShouldDeleteNonConflicted(revision.Flags) == false)
                    {
                        return false;
                    }
                }

                return true;
            }

            private bool ShouldDeleteConflicted(DateTime revisionLastModified, DocumentFlags revisionFlags)
            {

                if (FinishedConflicted)
                    return false;

                if (ConflictedReachedAge(revisionLastModified))
                {
                    FinishedConflicted = true;
                    return false;
                }

                _conflictDeletedCount++;
                FinishedConflicted |= ConflictedReachedMinimumToKeep();
                return true;
            }

            private bool ShouldDeleteNonConflicted(DocumentFlags revisionFlags)
            {

                if (FinishedRegular)
                {
                    return false;
                }
                if (_skipForceCreated && revisionFlags.Contain(DocumentFlags.ForceCreated))
                {
                    _skippedForceCreated++;
                    FinishedRegular |= AllRegularAreDeleted();
                    return false;
                }

                _regularDeletedCount++;
                FinishedRegular |= AllRegularAreDeleted();
                return true;
            }

            private bool AllRegularAreDeleted() => _regularCount - (_regularDeletedCount + _skippedForceCreated) == 0;

            private bool ConflictedReachedMinimumToKeep() => _minimumConflictRevisionsToKeep.HasValue && _conflictCount - _conflictDeletedCount <= _minimumConflictRevisionsToKeep.Value;

            private bool ConflictedReachedAge(DateTime revLastModify)
            {
                if (_config.MinimumRevisionAgeToKeep.HasValue)
                {
                    var diff = _databaseTime - revLastModify;
                    if (diff <= _config.MinimumRevisionAgeToKeep.Value) // reached not out of date
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        [Flags]
        private enum HandleConflictRevisionsFlags
        {
            None = 0,
            Conflicted = 1,
            Regular = 1 << 1,
            ForceCreated = 1 << 2
        }


        private IEnumerable<Document> GetRevisionsForConflict(
            DocumentsOperationContext context, Table table, Slice prefixSlice,
            NonPersistentDocumentFlags nonPersistentFlags, bool skipForceCreated, long revisionCount, bool documentDeleted,
            DeleteOldRevisionsResult result)
        {
            var handlingFlags = HandleConflictRevisionsFlags.Conflicted;
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByEnforceRevisionConfiguration))
            {
                handlingFlags |= HandleConflictRevisionsFlags.Regular;
                if (skipForceCreated == false)
                    handlingFlags |= HandleConflictRevisionsFlags.ForceCreated;
            }

            var databaseTime = _database.Time.GetUtcNow();


            var conflictRevisionsCount = GetConflictRevisionsCount(context, table, prefixSlice);

            var state = new ConflictedRevisionsDeletionState(revisionCount, conflictRevisionsCount, ConflictConfiguration.Default,
                handlingFlags, databaseTime, documentDeleted);

            result.HasMore = false;

            while (true)
            {
                var ended = true;
                foreach (var read in table.SeekForwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, result.Skip, startsWith: true))
                {
                    if (state.ReachedMaximumRevisionsToDeleteUponDocumentUpdate())
                    {
                        result.HasMore = true;
                        yield break;
                    }

                    if (state.FinishedRegular && state.FinishedConflicted)
                    {
                        yield break;
                    }

                    var tvr = read.Result.Reader;
                    var revision = TableValueToRevision(context, ref tvr, DocumentFields.ChangeVector | DocumentFields.LowerId);

                    if (state.ShouldDelete(revision) == false)
                    {
                        context.Transaction.ForgetAbout(revision);
                        revision.Dispose();
                        result.Skip++;
                        continue;
                    }

                    yield return revision;
                    ended = false;
                    break;
                }

                if (ended)
                {
                    yield break;
                }
            }
        }

        private long GetConflictRevisionsCount(
            DocumentsOperationContext context, Table table, Slice prefixSlice)
        {
            long conflictCount = 0;
            foreach (var read in table.SeekForwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, skip: 0, startsWith: true))
            {
                var tvr = read.Result.Reader;
                using (var revision = TableValueToRevision(context, ref tvr, DocumentFields.Default))
                {
                    if (revision.Flags.Contain(DocumentFlags.Conflicted) || revision.Flags.Contain(DocumentFlags.Resolved))
                        conflictCount++;

                    context.Transaction.ForgetAbout(revision);
                }
            }

            return conflictCount;
        }


        private IEnumerable<Document> GetAllRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice,
            long? maxDeletesUponUpdate, bool skipForceCreated, DeleteOldRevisionsResult result)
        {
            var deleted = 0L;

            while (true)
            {
                var ended = true;

                foreach (var read in table.SeekForwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, result.Skip, startsWith: true))
                {
                    if (maxDeletesUponUpdate.HasValue && deleted >= maxDeletesUponUpdate.Value)
                    {
                        result.HasMore = true;
                        yield break;
                    }

                    var tvr = read.Result.Reader;
                    var revision = TableValueToRevision(context, ref tvr, DocumentFields.ChangeVector | DocumentFields.LowerId);

                    if (skipForceCreated && revision.Flags.Contain(DocumentFlags.ForceCreated))
                    {
                        context.Transaction.ForgetAbout(revision);
                        revision.Dispose();
                        result.Skip++;
                        continue;
                    }

                    yield return revision;

                    deleted++;

                    ended = false;
                    break;
                }

                if (ended)
                    break;
            }
        }

        internal static void CreateRevisionTombstoneKeySlice(DocumentsOperationContext context, string documentId, string changeVector, out Slice changeVectorSlice, out Slice keySlice, List<IDisposable> toDispose)
        {
            toDispose.Add(DocumentIdWorker.GetSliceFromId(context, documentId, out var documentIdSlice));
            toDispose.Add(CreateRevisionTombstoneKeySlice(context, documentIdSlice, changeVector, out changeVectorSlice, out keySlice));
        }

        private static unsafe IDisposable CreateRevisionTombstoneKeySlice(DocumentsOperationContext context, Slice documentIdSlice, string changeVector, out Slice changeVectorSlice, out Slice keySlice)
        {
            var toDispose = new List<IDisposable>
            {
                Slice.From(context.Allocator, changeVector, out changeVectorSlice),
                context.Allocator.Allocate(documentIdSlice.Size + changeVectorSlice.Size + 1, out var keyBuffer),
                Slice.External(context.Allocator, keyBuffer.Ptr, keyBuffer.Length, out keySlice)
            };

            documentIdSlice.CopyTo(keyBuffer.Ptr);
            int pos = documentIdSlice.Size;
            keyBuffer.Ptr[pos++] = SpecialChars.RecordSeparator;
            changeVectorSlice.CopyTo(keyBuffer.Ptr + pos);

            return new DisposableAction(() =>
            {
                foreach (var item in toDispose)
                {
                    item.Dispose();
                }
            });
        }

        public void DeleteRevision(DocumentsOperationContext context, Slice key, string collection, string changeVector, long lastModifiedTicks, Slice changeVectorSlice)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

            long revisionEtag;

            if (table.ReadByKey(changeVectorSlice, out TableValueReader tvr))
            {
                EnsureValidRevisionTable(context, changeVectorSlice, ref table, ref tvr);

                using (TableValueToSlice(context, (int)RevisionsTable.LowerId, ref tvr, out Slice lowerId))
                using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    IncrementCountOfRevisions(context, prefixSlice, -1);
                }

                revisionEtag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr);

                table.Delete(tvr.Id);
            }
            else
            {
                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.TombstonesSchema, RevisionsTombstonesSlice);
                if (tombstoneTable.VerifyKeyExists(key))
                    return;

                // we need to generate a unique etag if we got a tombstone revisions from replication,
                // but we don't want to mess up the order of events so the delete revision etag we use is negative
                revisionEtag = _documentsStorage.GenerateNextEtagForReplicatedTombstoneMissingDocument(context);
            }
            CreateTombstone(context, key, revisionEtag, collectionName, changeVector, lastModifiedTicks);
        }

        private unsafe void CreateTombstone(DocumentsOperationContext context, Slice keySlice, long revisionEtag,
            CollectionName collectionName, string changeVector, long lastModifiedTicks, DocumentFlags flags = DocumentFlags.None)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.TombstonesSchema, RevisionsTombstonesSlice);
            if (table.VerifyKeyExists(keySlice))
                return; // revisions (and revisions tombstones) are immutable, we can safely ignore this

            var newEtag = _documentsStorage.GenerateNextEtag();

            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            using (Slice.From(context.Allocator, changeVector, out var cv))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(Bits.SwapBytes(revisionEtag));
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((byte)Tombstone.TombstoneType.Revision);
                tvb.Add(collectionSlice);
                tvb.Add((int)flags);
                tvb.Add(cv.Content.Ptr, cv.Size);
                tvb.Add(lastModifiedTicks);
                table.Set(tvb);
            }
        }

        internal static long IncrementCountOfRevisions(DocumentsOperationContext context, Slice prefixedLowerId, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            return numbers.Increment(prefixedLowerId, delta);
        }

        public void Delete(DocumentsOperationContext context, string id, Slice lowerId, CollectionName collectionName, ChangeVector changeVector,
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags, DocumentFlags flags)
        {
            using (DocumentIdWorker.GetStringPreserveCase(context, id, out Slice idPtr))
            {
                var deleteRevisionDocument = context.ReadObject(new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = collectionName.Name
                    }
                }, "RevisionsBin");
                Delete(context, lowerId, idPtr, id, collectionName, deleteRevisionDocument, changeVector, lastModifiedTicks, nonPersistentFlags, flags);
            }
        }

        public void Delete(DocumentsOperationContext context, string id, BlittableJsonReaderObject deleteRevisionDocument,
            DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, ChangeVector changeVector, long lastModifiedTicks)
        {
            BlittableJsonReaderObject.AssertNoModifications(deleteRevisionDocument, id, assertChildren: true);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, deleteRevisionDocument);
                Delete(context, lowerId, idPtr, id, collectionName, deleteRevisionDocument, changeVector, lastModifiedTicks, nonPersistentFlags, flags);
            }
        }

        private unsafe void Delete(DocumentsOperationContext context, Slice lowerId, Slice idSlice, string id, CollectionName collectionName,
            BlittableJsonReaderObject deleteRevisionDocument, ChangeVector changeVector,
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags, DocumentFlags flags)
        {
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.SkipRevisionCreation))
                return;

            Debug.Assert(changeVector != null, "Change vector must be set");
            var hadRevisions = flags.Contain(DocumentFlags.HasRevisions);
            flags = flags.Strip(DocumentFlags.HasAttachments);
            flags |= DocumentFlags.HasRevisions;

            var fromReplication = nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication);
            var fromResharding = nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResharding);

            var configuration = GetRevisionsConfiguration(collectionName.Name, flags);
            if (configuration.Disabled && hadRevisions == false && fromReplication == false)
                return;

            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

            using (Slice.From(context.Allocator, changeVector.Version, out var changeVectorSlice))
            {
                var revisionExists = table.ReadByKey(changeVectorSlice, out var tvr);
                if (revisionExists)
                {
                    MarkRevisionsAsConflictedIfNeeded(context, lowerId, idSlice, flags, tvr, table, changeVectorSlice);
                    return;
                }

                using var _ = GetKeyPrefix(context, lowerId, out Slice lowerIdPrefix);

                if (configuration.PurgeOnDelete && (fromResharding || fromReplication == false))
                {
                    DeleteOldRevisions(context, table, lowerIdPrefix, collectionName, configuration,
                        NonPersistentDocumentFlags.None,
                        changeVector, lastModifiedTicks, documentDeleted: true, skipForceCreated: false, flags);
                    return;
                }

                PutFromRevisionIfChangeVectorIsGreater(context, null, id, changeVector, lastModifiedTicks, flags, nonPersistentFlags, collectionName);

                var newEtag = _database.DocumentsStorage.GenerateNextEtag();
                var newEtagSwapBytes = Bits.SwapBytes(newEtag);

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);
                    tvb.Add(lowerId);
                    tvb.Add(SpecialChars.RecordSeparator);
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(idSlice);
                    tvb.Add(deleteRevisionDocument.BasePointer, deleteRevisionDocument.Size);
                    tvb.Add((int)(DocumentFlags.DeleteRevision | flags));
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(lastModifiedTicks);
                    tvb.Add(context.GetTransactionMarker());
                    if (flags.Contain(DocumentFlags.Resolved))
                    {
                        tvb.Add((int)DocumentFlags.Resolved);
                    }
                    else
                    {
                        tvb.Add(0);
                    }

                    tvb.Add(Bits.SwapBytes(lastModifiedTicks));
                    table.Insert(tvb);
                }

                IncrementCountOfRevisions(context, lowerIdPrefix, 1);
                DeleteOldRevisions(context, table, lowerIdPrefix, collectionName, configuration, nonPersistentFlags, changeVector, lastModifiedTicks,
                    documentDeleted: true, skipForceCreated: false);
            }
        }

        private void MarkRevisionsAsConflictedIfNeeded(DocumentsOperationContext context, Slice lowerId, Slice idSlice, DocumentFlags flags, TableValueReader tvr, Table table,
            Slice changeVectorSlice)
        {
            // Revisions are immutable, but if there was a conflict we need to update the flags accordingly with the `Conflicted` flag.
            if (flags.Contain(DocumentFlags.Conflicted))
            {
                var currentFlags = TableValueToFlags((int)RevisionsTable.Flags, ref tvr);
                if (currentFlags.Contain(DocumentFlags.Conflicted) == false)
                {
                    MarkRevisionAsConflicted(context, tvr, table, changeVectorSlice, lowerId, idSlice);
                }
            }
        }

        private unsafe void MarkRevisionAsConflicted(DocumentsOperationContext context, TableValueReader tvr, Table table, Slice changeVectorSlice, Slice lowerId, Slice idSlice)
        {
            EnsureValidRevisionTable(context, changeVectorSlice, ref table, ref tvr);

            var revisionCopy = context.GetMemory(tvr.Size);
            // we have to copy it to the side because we might do a defrag during update, and that
            // can cause corruption if we read from the old value (which we just deleted)
            Memory.Copy(revisionCopy.Address, tvr.Pointer, tvr.Size);
            var copyTvr = new TableValueReader(revisionCopy.Address, tvr.Size);

            var revision = TableValueToRevision(context, ref copyTvr);
            var flags = revision.Flags | DocumentFlags.Conflicted;
            var newEtag = _database.DocumentsStorage.GenerateNextEtag();
            var deletedEtag = TableValueToEtag((int)RevisionsTable.DeletedEtag, ref tvr);
            var resolvedFlag = TableValueToFlags((int)RevisionsTable.Resolved, ref tvr);

            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);
                tvb.Add(lowerId);
                tvb.Add(SpecialChars.RecordSeparator);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(idSlice);
                tvb.Add(revision.Data.BasePointer, revision.Data.Size);
                tvb.Add((int)flags);
                tvb.Add(Bits.SwapBytes(deletedEtag));
                tvb.Add(revision.LastModified.Ticks);
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((int)resolvedFlag);
                tvb.Add(Bits.SwapBytes(revision.LastModified.Ticks));
                table.Set(tvb);
            }
        }

        private void EnsureValidRevisionTable(DocumentsOperationContext context, Slice changeVectorSlice, ref Table table, ref TableValueReader tvr)
        {
            if (table.IsOwned(tvr.Id) == false)
            {
                // We request to update/remove revision with the wrong collection
                var revision = TableValueToRevision(context, ref tvr);
                var currentCollection = _documentsStorage.ExtractCollectionName(context, revision.Data);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Expected revision '{revision.Id}' with change vector '{revision.ChangeVector}' from table '{table.Name}' but revision is of collection '{currentCollection.Name}'");

                table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, currentCollection);

                if (table.IsOwned(tvr.Id) == false || table.ReadByKey(changeVectorSlice, out tvr) == false) // this shouldn't happened
                    throw new VoronErrorException(
                        $"Failed to get revision '{revision.Id}' with change vector '{revision.ChangeVector}' of collection '{currentCollection}' from table '{table.Name}'. " +
                        "This should not happen and is likely a bug.");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe ByteStringContext.InternalScope GetKeyPrefix(DocumentsOperationContext context, Slice lowerId, out Slice prefixSlice)
        {
            return GetKeyPrefix(context, lowerId.Content.Ptr, lowerId.Size, out prefixSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe ByteStringContext.InternalScope GetKeyPrefix(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, out Slice prefixSlice)
        {
            var scope = context.Allocator.Allocate(lowerIdSize + 1, out ByteString keyMem);

            Memory.Copy(keyMem.Ptr, lowerId, lowerIdSize);
            keyMem.Ptr[lowerIdSize] = SpecialChars.RecordSeparator;

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        private static ByteStringContext.InternalScope GetLastKey(DocumentsOperationContext context, Slice lowerId, out Slice prefixSlice)
        {
            return GetKeyWithEtag(context, lowerId, long.MaxValue, out prefixSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetKeyWithEtag(DocumentsOperationContext context, Slice lowerId, long etag, out Slice prefixSlice)
        {
            var scope = context.Allocator.Allocate(lowerId.Size + 1 + sizeof(long), out ByteString keyMem);

            Memory.Copy(keyMem.Ptr, lowerId.Content.Ptr, lowerId.Size);
            keyMem.Ptr[lowerId.Size] = SpecialChars.RecordSeparator;

            var maxValue = Bits.SwapBytes(etag);
            Memory.Copy(keyMem.Ptr + lowerId.Size + 1, (byte*)&maxValue, sizeof(long));

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        private static long CountOfRevisions(DocumentsOperationContext context, Slice prefix)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            return numbers.Read(prefix)?.Reader.ReadLittleEndianInt64() ?? 0;
        }

        public Document GetRevisionBefore(DocumentsOperationContext context, string id, DateTime max)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            using (GetLastKey(context, lowerId, out Slice lastKey))
            {
                // Here we assume a reasonable number of revisions and scan the entire history
                // This is because we want to handle out of order revisions from multiple nodes so the local etag
                // order is different than the last modified order
                Document result = null;
                var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
                foreach (var tvr in table.SeekBackwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey, 0))
                {
                    var lastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref tvr.Result.Reader);
                    if (lastModified > max)
                        continue;

                    if (result == null ||
                        result.LastModified < lastModified)
                    {
                        result = TableValueToRevision(context, ref tvr.Result.Reader);
                    }
                }
                return result;
            }
        }

        private unsafe Document GetRevisionBefore(DocumentsOperationContext context,
            Parameters parameters,
            string id,
            RevertResult progressResult)
        {
            var foundAfter = false;

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            using (GetLastKey(context, lowerId, out Slice lastKey))
            {
                // Here we assume a reasonable number of revisions and scan the entire history
                // This is because we want to handle out of order revisions from multiple nodes so the local etag
                // order is different than the last modified order
                Document result = null;
                Document prev = null;
                string collection = null;

                var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
                foreach (var tvr in table.SeekBackwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey, 0))
                {
                    if (collection == null)
                    {
                        var ptr = tvr.Result.Reader.Read((int)RevisionsTable.Document, out var size);
                        var data = new BlittableJsonReaderObject(ptr, size, context);
                        collection = _documentsStorage.ExtractCollectionName(context, data).Name;
                    }

                    var etag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr.Result.Reader);
                    if (etag > parameters.EtagBarrier)
                    {
                        progressResult.Warn(id, "This document wouldn't be reverted, because it changed after the revert progress started.");
                        return null;
                    }

                    var lastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref tvr.Result.Reader);
                    if (lastModified > parameters.Before)
                    {
                        foundAfter = true;
                        continue;
                    }

                    if (lastModified < parameters.MinimalDate)
                    {
                        // this is a very old revision, and we should stop here
                        if (result == null)
                        {
                            // we will take this old revision if no other was found
                            result = TableValueToRevision(context, ref tvr.Result.Reader);
                            prev = result;
                        }
                        break;
                    }

                    if (result == null)
                    {
                        result = TableValueToRevision(context, ref tvr.Result.Reader);
                        prev = result;
                        continue;
                    }

                    if (result.LastModified < lastModified)
                    {
                        prev = result;
                        result = TableValueToRevision(context, ref tvr.Result.Reader);
                        continue;
                    }

                    if (prev.LastModified < lastModified)
                    {
                        prev = TableValueToRevision(context, ref tvr.Result.Reader);
                    }
                }

                if (prev != result)
                {
                    // put at 8:50
                    // conflict at 9:10
                    // resolved at 9:30

                    // revert to 9:00 should work
                    // revert to 9:20 should fail

                    if (prev.Flags.Contain(DocumentFlags.Conflicted) && result.Flags.Contain(DocumentFlags.Conflicted))
                    {
                        // found two successive conflicted revisions, which means we were in a conflicted state.
                        progressResult.Warn(id, $"Skip revert, since the document was conflicted during '{parameters.Before}'.");
                        return null;
                    }
                }

                if (foundAfter == false)
                    return null; // nothing do to, no changes were found

                if (result == null) // no revision before POT was found
                {
                    var count = CountOfRevisions(context, prefixSlice);
                    var revisionsToKeep = GetRevisionsConfiguration(collection).MinimumRevisionsToKeep;
                    if (revisionsToKeep == null || count < revisionsToKeep)
                    {
                        var copy = lowerId.Clone(context.Allocator);

                        // document was created after POT so we need to delete it.
                        return new Document
                        {
                            Flags = DocumentFlags.DeleteRevision,
                            LowerId = context.AllocateStringValue(null, copy.Content.Ptr, copy.Size),
                            Id = context.GetLazyString(id)
                        };
                    }

                    var first = table.SeekOneForwardFromPrefix(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice);
                    if (first == null)
                        return null;

                    // document reached max number of revisions. So we take the oldest.
                    progressResult.Warn(id,
                        $"Reverted to oldest revision, since no revision prior to '{parameters.Before}' was found and you reached the maximum number of revisions ({count}).");
                    return TableValueToRevision(context, ref first.Reader);
                }

                return result;
            }
        }

        public Task<IOperationResult> EnforceConfigurationAsync(Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return EnforceConfigurationAsync(onProgress, includeForceCreated: true, null, token: token);
        }

        public Task<IOperationResult> EnforceConfigurationAsync(Action<IOperationProgress> onProgress, bool includeForceCreated, OperationCancelToken token)
        {
            return EnforceConfigurationAsync(onProgress, includeForceCreated, null, token: token);
        }

        public async Task<IOperationResult> EnforceConfigurationAsync(Action<IOperationProgress> onProgress,
           bool includeForceCreated, // include ForceCreated revisions on deletion in case of no revisions configuration (only conflict revisions config is exist).
           HashSet<string> collections,
           OperationCancelToken token)
        {
            var result = new EnforceConfigurationResult();
            await PerformRevisionsOperationAsync(onProgress, result,
                (ids, res, tk) => new EnforceRevisionConfigurationCommand(this, ids, res, includeForceCreated, tk),
                collections, token);

            return result;
        }

        public async Task<IOperationResult> AdoptOrphanedAsync(Action<IOperationProgress> onProgress,
            HashSet<string> collections,
            OperationCancelToken token)
        {
            var result = new AdoptOrphanedRevisionsResult();
            await PerformRevisionsOperationAsync(onProgress, result,
                (ids, res, tk) => new AdoptOrphanedRevisionsCommand(this, ids, result, tk),
                collections: collections, token);

            return result;
        }

        public async Task<IOperationResult> AdoptOrphanedAsync(Action<IOperationProgress> onProgress,
            OperationCancelToken token)
        {
            var result = new AdoptOrphanedRevisionsResult();
            await PerformRevisionsOperationAsync(onProgress, result,
                (ids, res, tk) => new AdoptOrphanedRevisionsCommand(this, ids, result, tk),
                collections: null, token);

            return result;
        }

        private bool CanContinueBatch(List<string> idsToCheck, TimeSpan elapsed, JsonOperationContext context)
        {
            if (idsToCheck.Count > 1024)
                return false;

            if (elapsed > MaxEnforceConfigurationSingleBatchTime)
                return false;

            if (context.AllocatedMemory > SizeLimitInBytes)
                return false;

            return true;
        }

        private async Task PerformRevisionsOperationAsync<TOperationResult>(
            Action<IOperationProgress> onProgress,
            TOperationResult result,
            Func<List<string>, TOperationResult, OperationCancelToken, RevisionsScanningOperationCommand<TOperationResult>> createCommand,
            HashSet<string> collections,
            OperationCancelToken token) where TOperationResult : OperationResult
        {
            if (collections != null)
            {
                if (collections.Comparer?.Equals(StringComparer.OrdinalIgnoreCase) == false)
                    throw new InvalidOperationException("'collections' hashset must have an 'OrdinalIgnoreCase' comparer");

                foreach (var collection in collections)
                {
                    if (string.IsNullOrEmpty(collection))
                        throw new InvalidOperationException("There is no collection with name which is empty string or 'null'.");
                }
            }

            var parameters = new Parameters
            {
                Before = DateTime.MinValue,
                MinimalDate = DateTime.MinValue,
                EtagBarrier = _documentsStorage.GenerateNextEtag(),
                OnProgress = onProgress
            };

            parameters.LastScannedEtag = parameters.EtagBarrier;

            var ids = new List<string>();
            var sw = Stopwatch.StartNew();

            // send initial progress
            parameters.OnProgress?.Invoke(result);

            var hasMore = true;
            while (hasMore)
            {
                hasMore = false;
                ids.Clear();
                token.Delay();
                sw.Restart();

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    using (ctx.OpenReadTransaction())
                    {
                        var tables = GetRevisionsTables(ctx, collections, parameters.LastScannedEtag);

                        foreach (var table in tables)
                        {
                            foreach (var tvr in table)
                            {
                                token.ThrowIfCancellationRequested();

                                var state = ShouldProcessNextRevisionId(ctx, ref tvr.Reader, parameters, result, out var id);
                                if (state == NextRevisionIdResult.Break)
                                    break;
                                if (state == NextRevisionIdResult.Continue)
                                {
                                    if (CanContinueBatch(ids, sw.Elapsed, ctx) == false)
                                    {
                                        hasMore = true;
                                        break;
                                    }
                                    else
                                        continue;
                                }

                                ids.Add(id);

                                if (CanContinueBatch(ids, sw.Elapsed, ctx) == false)
                                {
                                    hasMore = true;
                                    break;
                                }
                            }

                            var moreWork = true;
                            while (moreWork)
                            {
                                token.Delay();
                                var cmd = createCommand(ids, result, token);
                                await _database.TxMerger.Enqueue(cmd);
                                moreWork = cmd.MoreWork;
                            }
                        }
                    }


                }
            }
        }

        private List<IEnumerable<TableValueHolder>> GetRevisionsTables(DocumentsOperationContext context, HashSet<string> collections, long lastScannedEtag)
        {
            var collectionsTables = new List<IEnumerable<TableValueHolder>>();

            if (collections == null)
            {
                var revisions = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
                var table = revisions.SeekBackwardFrom(RevisionsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice], lastScannedEtag);
                collectionsTables.Add(table);
            }
            else
            {
                foreach (var collection in collections)
                {
                    var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false) ?? new CollectionName(collection);
                    var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
                    var revisions = context.Transaction.InnerTransaction.OpenTable(RevisionsSchema, tableName);
                    if (revisions == null) // there is no revisions for that collection
                    {
                        continue;
                    }

                    var table = revisions.SeekBackwardFrom(RevisionsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice], lastScannedEtag);

                    collectionsTables.Add(table);
                }
            }

            return collectionsTables;
        }

        private static readonly RevisionsCollectionConfiguration ZeroConfiguration = new RevisionsCollectionConfiguration
        {
            MinimumRevisionsToKeep = 0
        };

        internal long EnforceConfigurationFor(DocumentsOperationContext context, string id, bool skipForceCreated, ref bool moreWork)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out var lowerId))
            using (GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
            {
                var collectionName = GetCollectionFor(context, lowerIdPrefix);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                    return 0;
                }

                var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                var newEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);
                var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;

                var local = _documentsStorage.GetDocumentOrTombstone(context, lowerId, throwOnConflict: false);
                var deletedDoc = local.Document == null;

                var configuration = GetRevisionsConfiguration(collectionName.Name, deleteRevisionsWhenNoCofiguration: true);

                var result = DeleteOldRevisions(context, table, lowerIdPrefix, collectionName, configuration,
                    NonPersistentDocumentFlags.ByEnforceRevisionConfiguration,
                    changeVector, lastModifiedTicks, deletedDoc, skipForceCreated);

                var needToDeleteMore = result.HasMore;
                var prevRevisionsCount = result.PreviousCount;
                var currentRevisionsCount = result.Remaining;

                if (needToDeleteMore && currentRevisionsCount > 0)
                    moreWork = true;

                if (currentRevisionsCount == 0)
                {
                    var res = _documentsStorage.GetDocumentOrTombstone(context, lowerId, throwOnConflict: false);
                    // need to strip the HasRevisions flag from the document/tombstone
                    if (res.Tombstone != null)
                        _documentsStorage.Delete(context, lowerId, id, null, nonPersistentFlags: NonPersistentDocumentFlags.ByEnforceRevisionConfiguration);

                    if (res.Document != null)
                        _documentsStorage.Put(context, id, null, res.Document.Data.Clone(context),
                            nonPersistentFlags: NonPersistentDocumentFlags.ByEnforceRevisionConfiguration);
                }

                return prevRevisionsCount - currentRevisionsCount;
            }
        }

        internal bool AdoptOrphanedFor(DocumentsOperationContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out var lowerId))
            using (GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
            {
                var collectionName = GetCollectionFor(context, lowerIdPrefix);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete revisions for '{id}' but no collection was found.");
                    return false;
                }


                if (ShouldAdoptRevision(context, lowerId, lowerIdPrefix, collectionName, out var table, out var lastRevision))
                {
                    var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;
                    CreateDeletedRevision(context, table, id, collectionName, lastModifiedTicks, lastRevision.Flags);
                    return true;
                }

                return false;
            }
        }

        private unsafe void CreateDeletedRevision(DocumentsOperationContext context, Table table, string id, CollectionName collectionName,
            long lastModifiedTicks, DocumentFlags flags)
        {
            var deleteRevisionDocument = context.ReadObject(new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = collectionName.Name
                }
            }, "RevisionsBin");

            var newEtag = _database.DocumentsStorage.GenerateNextEtag();
            var changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);

            Debug.Assert(changeVector != null, "Change vector must be set");
            flags = flags.Strip(DocumentFlags.HasAttachments);
            flags |= DocumentFlags.HasRevisions;

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idSlice))
            using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
            {
                using var _ = GetKeyPrefix(context, lowerId, out Slice lowerIdPrefix);
                var newEtagSwapBytes = Bits.SwapBytes(newEtag);
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);
                    tvb.Add(lowerId);
                    tvb.Add(SpecialChars.RecordSeparator);
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(idSlice);
                    tvb.Add(deleteRevisionDocument.BasePointer, deleteRevisionDocument.Size);
                    tvb.Add((int)(DocumentFlags.DeleteRevision | flags));
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(lastModifiedTicks);
                    tvb.Add(context.GetTransactionMarker());
                    if (flags.Contain(DocumentFlags.Resolved))
                    {
                        tvb.Add((int)DocumentFlags.Resolved);
                    }
                    else
                    {
                        tvb.Add(0);
                    }

                    tvb.Add(Bits.SwapBytes(lastModifiedTicks));
                    table.Insert(tvb);
                }

                IncrementCountOfRevisions(context, lowerIdPrefix, 1);
            }
        }

        private sealed class Parameters
        {
            public DateTime Before;
            public DateTime MinimalDate;
            public long EtagBarrier;
            public long LastScannedEtag;
            public readonly HashSet<string> ScannedIds = new HashSet<string>();
            public Action<IOperationProgress> OnProgress;
        }

        public async Task<IOperationResult> RevertRevisions(DateTime before, TimeSpan window, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return await RevertRevisions(before, window, onProgress, collections: null, token);
        }

        public (Document Revision, TableValueReader TableValueReader) VerifyCvAndGetRevision(DocumentsOperationContext context, string id, string cv)
        {
            if (id == null)
                throw new ArgumentException("Document id is null");

            if (id == string.Empty)
                throw new ArgumentException("Document id is an empty string");

            if (cv == null)
                throw new ArgumentException("Change Vector is null");

            if (id == string.Empty)
                throw new ArgumentException("Change Vector is an empty string");

            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
            using (Slice.From(context.Allocator, cv, out var cvSlice))
            {
                if (table.ReadByKey(cvSlice, out TableValueReader tvr) == false)
                {
                    throw new InvalidOperationException($"Revision with the cv {cv} doesn't belong to the doc \"{id}\"");
                }

                var revision = TableValueToRevision(context, ref tvr, DocumentFields.Id | DocumentFields.LowerId | DocumentFields.ChangeVector);

                if (revision.Id != id)
                {
                    throw new InvalidOperationException($"Revision with the cv {cv} doesn't belong to the doc \"{id}\" but to the doc \"{revision.Id}\"");
                }

                return (revision, tvr);
            }
        }

        public async Task RevertDocumentsToRevisions(Dictionary<string, string> idsToChangevectors, OperationCancelToken token)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var revisions = new List<Document>();
                var tvrs = new List<TableValueReader>();

                // Verify matching for all ids and cvs
                foreach (var (id, cv) in idsToChangevectors)
                {
                    var (revision, tvr) = _database.DocumentsStorage.RevisionsStorage.VerifyCvAndGetRevision(context, id, cv);
                    revisions.Add(revision);
                    tvrs.Add(tvr);
                }

                // Get Data After Verification
                for (int i = 0; i < revisions.Count; i++)
                {
                    var revision = revisions[i];
                    var tvr = tvrs[i];
                    revision.Data = GetRevisionData(context, ref tvr);
                }

                await WriteRevertedRevisions(revisions, token);
            }
        }


        public async Task<IOperationResult> RevertRevisions(DateTime before, TimeSpan window, Action<IOperationProgress> onProgress, HashSet<string> collections, OperationCancelToken token)
        {
            var result = new RevertResult();
            var etagBarrier = _documentsStorage.GenerateNextEtag(); // every change after this etag, will _not_ be reverted.
            var minimalDate = before.Add(-window); // since the documents/revisions are not sorted by date, stop searching if we reached this date.

            if (collections == null) // revert all collections
            {
                var list = new List<Document>();
                await RevertRevisionsInternal(list, collection: null, before, minimalDate, etagBarrier, onProgress, result, token);
            }
            else
            {
                if (collections.Comparer != null && collections.Comparer.Equals(StringComparer.OrdinalIgnoreCase) == false)
                {
                    throw new InvalidOperationException("'collections' hashset must have an 'OrdinalIgnoreCase' comparer");
                }
                foreach (var collection in collections)
                {
                    var list = new List<Document>();
                    if (collection == null)
                    {
                        var msg = "Tried to revert revisions in collection that is null";
                        if (_logger.IsInfoEnabled)
                            _logger.Info(msg);
                        result.WarnAboutFailedCollection(msg);
                        continue;
                    }

                    await RevertRevisionsInternal(list, collection, before, minimalDate, etagBarrier, onProgress, result, token);
                }
            }

            return result;
        }

        private async Task RevertRevisionsInternal(List<Document> list, string collection, DateTime before, DateTime minimalDate, long etagBarrier, Action<IOperationProgress> onProgress, RevertResult result, OperationCancelToken token)
        {
            var parameters = new Parameters
            {
                Before = before,
                MinimalDate = minimalDate,
                EtagBarrier = etagBarrier,
                OnProgress = onProgress,
                LastScannedEtag = etagBarrier
            };

            // send initial progress
            parameters.OnProgress?.Invoke(result);

            var hasMore = true;
            while (hasMore)
            {
                token.Delay();

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeCtx))
                {
                    hasMore = PrepareRevertedRevisions(writeCtx, parameters, result, list, collection, token);
                    await WriteRevertedRevisions(list, token);
                }
            }
        }

        public async Task WriteRevertedRevisions(List<Document> list, OperationCancelToken token)
        {
            if (list.Count == 0)
                return;

            await _database.TxMerger.Enqueue(new RevertDocumentsCommand(list, token));

            list.Clear();
        }

        private bool PrepareRevertedRevisions(DocumentsOperationContext writeCtx, Parameters parameters, RevertResult result, List<Document> list, string collection, OperationCancelToken token)
        {
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
            using (readCtx.OpenReadTransaction())
            {
                IEnumerable<Table.TableValueHolder> tvrs = null;
                if (collection != null)
                {
                    var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
                    if (collectionName == null)
                    {
                        var msg = $"Tried to revert revisions in the collection '{collection}' which does not exist";
                        if (_logger.IsInfoEnabled)
                            _logger.Info(msg);
                        result.WarnAboutFailedCollection(msg);
                        return false;
                    }
                    var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
                    var revisions = readCtx.Transaction.InnerTransaction.OpenTable(RevisionsSchema, tableName);
                    if (revisions == null)
                    {
                        var msg = $"Collection '{collection}' doesn't have any revisions.";
                        if (_logger.IsInfoEnabled)
                            _logger.Info(msg);
                        result.WarnAboutFailedCollection(msg);
                        return false;
                    }

                    tvrs = revisions.SeekBackwardFrom(RevisionsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice], parameters.LastScannedEtag);
                }
                else
                {
                    var revisions = new Table(RevisionsSchema, readCtx.Transaction.InnerTransaction);
                    tvrs = revisions.SeekBackwardFrom(RevisionsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice], parameters.LastScannedEtag);
                }

                foreach (var tvr in tvrs)
                {
                    token.ThrowIfCancellationRequested();

                    var state = ShouldProcessNextRevisionId(readCtx, ref tvr.Reader, parameters, result, out var id);
                    if (state == NextRevisionIdResult.Break)
                        break;
                    if (state == NextRevisionIdResult.Continue)
                        continue;

                    RestoreRevision(readCtx, writeCtx, parameters, id, result, list);

                    if (readCtx.AllocatedMemory + writeCtx.AllocatedMemory > SizeLimitInBytes)
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        private enum NextRevisionIdResult
        {
            Break,
            Continue,
            Found
        }

        private NextRevisionIdResult ShouldProcessNextRevisionId(DocumentsOperationContext context, ref TableValueReader reader, Parameters parameters, OperationResult result, out LazyStringValue id)
        {
            result.ScannedRevisions++;

            if (result.ScannedRevisions % 1024 == 0)
                parameters.OnProgress?.Invoke(result);

            id = TableValueToId(context, (int)RevisionsTable.Id, ref reader);
            var etag = TableValueToEtag((int)RevisionsTable.Etag, ref reader);
            parameters.LastScannedEtag = etag;

            if (parameters.ScannedIds.Add(id) == false)
                return NextRevisionIdResult.Continue;

            result.ScannedDocuments++;

            if (etag > parameters.EtagBarrier)
            {
                result.Warn(id, "This document wouldn't be processed, because it changed after the process started.");
                return NextRevisionIdResult.Continue;
            }

            if (_documentsStorage.ConflictsStorage.HasConflictsFor(context, id))
            {
                result.Warn(id, "The document is conflicted and wouldn't be processed.");
                return NextRevisionIdResult.Continue;
            }

            var date = TableValueToDateTime((int)RevisionsTable.LastModified, ref reader);
            if (date < parameters.MinimalDate)
                return NextRevisionIdResult.Break;

            if (result.ScannedDocuments % 1024 == 0)
                parameters.OnProgress?.Invoke(result);

            return NextRevisionIdResult.Found;
        }

        private void RestoreRevision(DocumentsOperationContext readCtx,
            DocumentsOperationContext writeCtx,
            Parameters parameters,
            LazyStringValue id,
            RevertResult result,
            List<Document> list)
        {
            var revision = GetRevisionBefore(readCtx, parameters, id, result);
            if (revision == null)
                return;

            result.RevertedDocuments++;

            revision.Data = revision.Flags.Contain(DocumentFlags.DeleteRevision) ? null : revision.Data?.Clone(writeCtx);
            revision.LowerId = writeCtx.GetLazyString(revision.LowerId);
            revision.Id = writeCtx.GetLazyString(revision.Id);

            list.Add(revision);
        }

        internal sealed class RevertDocumentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly List<Document> _list;
            private readonly CancellationToken _token;

            public RevertDocumentsCommand(List<Document> list, OperationCancelToken token)
            {
                _list = list;
                _token = token.Token;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var documentsStorage = context.DocumentDatabase.DocumentsStorage;
                foreach (var document in _list)
                {
                    _token.ThrowIfCancellationRequested();
                    var flags = document.Flags.Strip(DocumentFlags.Revision | DocumentFlags.Conflicted | DocumentFlags.Resolved | DocumentFlags.FromClusterTransaction | DocumentFlags.FromReplication) | DocumentFlags.Reverted;

                    if (document.Data != null)
                    {
                        CollectionName collectionName = RemoveOldMetadataInfo(context, documentsStorage, document);
                        InsertNewMetadataInfo(context, documentsStorage, document, collectionName);

                        documentsStorage.Put(context, document.Id, null, document.Data, flags: flags);
                    }
                    else
                    {
                        using (DocumentIdWorker.GetSliceFromId(context, document.Id, out Slice lowerId))
                        {
                            documentsStorage.Delete(context, lowerId, document.Id, null, changeVector: documentsStorage.GetNewChangeVector(context).ChangeVector, newFlags: flags);
                        }
                    }
                }

                return _list.Count;
            }

            private static void InsertNewMetadataInfo(DocumentsOperationContext context, DocumentsStorage documentsStorage, Document document, CollectionName collectionName)
            {
                documentsStorage.AttachmentsStorage.PutAttachmentRevert(context, document.Id, document.Data, out bool has);
                RevertCounters(context, documentsStorage, document, collectionName);

                document.Data = RevertSnapshotFlags(context, document.Data, document.Id);
            }

            private static void RevertCounters(DocumentsOperationContext context, DocumentsStorage documentsStorage, Document document, CollectionName collectionName)
            {
                if (document.TryGetMetadata(out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(Constants.Documents.Metadata.RevisionCounters, out BlittableJsonReaderObject counters))
                {
                    var counterNames = counters.GetPropertyNames();

                    foreach (var cn in counterNames)
                    {
                        var val = counters.TryGetMember(cn, out object value);
                        documentsStorage.CountersStorage.PutCounter(context, document.Id, collectionName.Name, cn, (long)value);
                    }
                }
            }

            private static CollectionName RemoveOldMetadataInfo(DocumentsOperationContext context, DocumentsStorage documentsStorage, Document document)
            {
                documentsStorage.AttachmentsStorage.DeleteAttachmentBeforeRevert(context, document.LowerId);
                var collectionName = documentsStorage.ExtractCollectionName(context, document.Data);
                documentsStorage.CountersStorage.DeleteCountersForDocument(context, document.Id, collectionName);

                return collectionName;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new RevertDocumentsCommandDto(_list);
            }
        }

        internal sealed class RevertDocumentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, RevertDocumentsCommand>
        {
            public readonly List<Document> List;

            public RevertDocumentsCommandDto(List<Document> list)
            {
                List = list;
            }

            public RevertDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new RevertDocumentsCommand(List, OperationCancelToken.None);
            }
        }

        public long GetRevisionsCount(DocumentsOperationContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                return GetRevisionsCount(context, prefixSlice);
            }
        }

        public long GetRevisionsCount(DocumentsOperationContext context, Slice id)
        {
            var count = CountOfRevisions(context, id);
            return count;
        }

        public (Document[] Revisions, long Count) GetRevisions(DocumentsOperationContext context, string id, long start, long take)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            using (GetLastKey(context, lowerId, out Slice lastKey))
            {
                var revisions = GetRevisions(context, prefixSlice, lastKey, start, take).ToArray();
                var count = CountOfRevisions(context, prefixSlice);
                return (revisions, count);
            }
        }

        private IEnumerable<Document> GetRevisions(DocumentsOperationContext context, Slice prefixSlice, Slice lastKey, long start, long take)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
            foreach (var tvr in table.SeekBackwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey, start))
            {
                if (take-- <= 0)
                    yield break;

                var document = TableValueToRevision(context, ref tvr.Result.Reader);
                yield return document;
            }
        }

        public void GetLatestRevisionsBinEntry(DocumentsOperationContext context, out string latestChangeVector)
        {
            latestChangeVector = null;
            foreach (var entry in GetRevisionsBinEntries(context, 0, 1))
            {
                latestChangeVector = entry.ChangeVector;
            }
        }

        public IEnumerable<Document> GetRevisionsBinEntries(DocumentsOperationContext context, long skip, long take)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);

            foreach (var tvr in table.SeekBackwardFrom(RevisionsSchema.Indexes[DeleteRevisionEtagSlice], null, Slices.AfterAllKeys, skip))
            {
                if (take-- <= 0)
                    yield break;

                var etag = TableValueToEtag((int)RevisionsTable.DeletedEtag, ref tvr.Result.Reader);
                if (etag == NotDeletedRevisionMarker)
                    yield break;

                using (TableValueToSlice(context, (int)RevisionsTable.LowerId, ref tvr.Result.Reader, out Slice lowerId))
                {
                    if (IsRevisionsBinEntry(context, table, lowerId, etag) == false)
                        continue;
                }

                yield return TableValueToRevision(context, ref tvr.Result.Reader);
            }
        }

        private bool IsRevisionsBinEntry(DocumentsOperationContext context, Table table, Slice lowerId, long revisionsBinEntryEtag)
        {
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            using (GetLastKey(context, lowerId, out Slice lastKey))
            {
                var tvr = table.SeekOneBackwardFrom(RevisionsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey);
                if (tvr == null)
                {
                    Debug.Assert(false, "Cannot happen.");
                    return true;
                }

                var etag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr.Reader);
                var flags = TableValueToFlags((int)RevisionsTable.Flags, ref tvr.Reader);
                Debug.Assert(revisionsBinEntryEtag <= etag, $"Revisions bin entry for '{lowerId}' etag candidate ({etag}) cannot meet a bigger etag ({revisionsBinEntryEtag}).");
                return (flags & DocumentFlags.DeleteRevision) == DocumentFlags.DeleteRevision && revisionsBinEntryEtag >= etag;
            }
        }

        public Document GetRevision(DocumentsOperationContext context, string changeVector)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);

            using (Slice.From(context.Allocator, changeVector, out var cv))
            {
                if (table.ReadByKey(cv, out TableValueReader tvr) == false)
                    return null;
                return TableValueToRevision(context, ref tvr);
            }
        }

        public IEnumerable<Document> GetRevisionsFrom(DocumentsOperationContext context, long etag, long take, DocumentFields fields = DocumentFields.All, EventHandler<InvalidOperationException> onCorruptedDataHandler = null)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction, onCorruptedDataHandler);

            foreach (var tvr in table.SeekForwardFrom(RevisionsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice], etag, 0))
            {
                if (take-- <= 0)
                    yield break;

                var document = TableValueToRevision(context, ref tvr.Reader, fields);
                yield return document;
            }
        }

        public IEnumerable<Document> GetRevisionsFrom(DocumentsOperationContext context, string collection, long etag, long take, DocumentFields fields = DocumentFields.All)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
            var table = context.Transaction.InnerTransaction.OpenTable(RevisionsSchema, tableName);
            if (table == null)
                yield break;

            foreach (var tvr in table.SeekForwardFrom(RevisionsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice], etag, 0))
            {
                if (take-- <= 0)
                    yield break;

                var document = TableValueToRevision(context, ref tvr.Reader, fields);
                yield return document;
            }
        }

        public long GetLastRevisionEtag(DocumentsOperationContext context, string collection)
        {
            Table.TableValueHolder result = null;
            if (LastRevision(context, collection, ref result) == false)
                return 0;

            return TableValueToEtag((int)RevisionsTable.Etag, ref result.Reader);
        }

        private bool LastRevision(DocumentsOperationContext context, string collection, ref Table.TableValueHolder result)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return false;

            var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
            var table = context.Transaction.InnerTransaction.OpenTable(RevisionsSchema, tableName);
            // ReSharper disable once UseNullPropagation
            if (table == null)
                return false;

            result = table.ReadLast(RevisionsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice]);
            if (result == null)
                return false;

            return true;
        }

        public IEnumerable<(Document Previous, Document Current)> GetCurrentAndPreviousRevisionsForSubscriptionsFrom(
            DocumentsOperationContext context,
            long etag,
            long start,
            long take)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);

            var iterator = table.SeekForwardFrom(RevisionsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice], etag, start);

            return GetCurrentAndPreviousRevisionsFrom(context, iterator, table, take);
        }

        public IEnumerable<(Document Previous, Document Current)> GetCurrentAndPreviousRevisionsForSubscriptionsFrom(
            DocumentsOperationContext context,
            CollectionName collectionName,
            long etag,
            long take)
        {
            var tableName = collectionName.GetTableName(CollectionTableType.Revisions);
            var table = context.Transaction.InnerTransaction.OpenTable(RevisionsSchema, tableName);

            var iterator = table?.SeekForwardFrom(RevisionsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice], etag, 0);

            return GetCurrentAndPreviousRevisionsFrom(context, iterator, table, take);
        }

        private IEnumerable<(Document Previous, Document Current)> GetCurrentAndPreviousRevisionsFrom(
            DocumentsOperationContext context,
            IEnumerable<Table.TableValueHolder> iterator,
            Table table,
            long take)
        {
            if (table == null)
                yield break;

            if (iterator == null)
                yield break;

            var docsSchemaIndex = _documentsStorage.RevisionsStorage.RevisionsSchema.Indexes[IdAndEtagSlice];

            foreach (var tvr in iterator)
            {
                if (take-- <= 0)
                    break;
                var current = TableValueToRevision(context, ref tvr.Reader);

                using (docsSchemaIndex.GetValue(context.Allocator, ref tvr.Reader, out var idAndEtag))
                using (Slice.External(context.Allocator, idAndEtag, idAndEtag.Size - sizeof(long), out var prefix))
                {
                    bool hasPrevious = false;
                    foreach (var prevTvr in table.SeekBackwardFrom(docsSchemaIndex, prefix, idAndEtag, 1))
                    {
                        var previous = TableValueToRevision(context, ref prevTvr.Result.Reader);

                        yield return (previous, current);
                        hasPrevious = true;
                        break;
                    }
                    if (hasPrevious)
                        continue;
                }

                yield return (null, current);
            }
        }

        internal static unsafe Document TableValueToRevision(JsonOperationContext context, ref TableValueReader tvr, DocumentFields fields = DocumentFields.All)
        {
            if (fields == DocumentFields.All)
            {
                return new Document
                {
                    StorageId = tvr.Id,
                    LowerId = TableValueToString(context, (int)RevisionsTable.LowerId, ref tvr),
                    Id = TableValueToId(context, (int)RevisionsTable.Id, ref tvr),
                    Etag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr),
                    LastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref tvr),
                    Flags = TableValueToFlags((int)RevisionsTable.Flags, ref tvr),
                    TransactionMarker = TableValueToShort((int)RevisionsTable.TransactionMarker, nameof(RevisionsTable.TransactionMarker), ref tvr),
                    ChangeVector = TableValueToChangeVector(context, (int)RevisionsTable.ChangeVector, ref tvr),
                    Data = new BlittableJsonReaderObject(tvr.Read((int)RevisionsTable.Document, out var size), size, context)
                };
            }

            return ParseRevisionPartial(context, ref tvr, fields);
        }

        internal static unsafe BlittableJsonReaderObject GetRevisionData(JsonOperationContext context, ref TableValueReader tvr)
        {
            return new BlittableJsonReaderObject(tvr.Read((int)RevisionsTable.Document, out var size), size, context);
        }

        private static unsafe Document ParseRevisionPartial(JsonOperationContext context, ref TableValueReader tvr, DocumentFields fields)
        {
            var result = new Document();

            if (fields.Contain(DocumentFields.LowerId))
                result.LowerId = TableValueToString(context, (int)RevisionsTable.LowerId, ref tvr);

            if (fields.Contain(DocumentFields.Id))
                result.Id = TableValueToId(context, (int)RevisionsTable.Id, ref tvr);

            if (fields.Contain(DocumentFields.Data))
                result.Data = new BlittableJsonReaderObject(tvr.Read((int)RevisionsTable.Document, out var size), size, context);

            if (fields.Contain(DocumentFields.ChangeVector))
                result.ChangeVector = TableValueToChangeVector(context, (int)RevisionsTable.ChangeVector, ref tvr);

            result.Etag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr);
            result.LastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref tvr);
            result.Flags = TableValueToFlags((int)RevisionsTable.Flags, ref tvr);
            result.StorageId = tvr.Id;
            result.TransactionMarker = TableValueToShort((int)RevisionsTable.TransactionMarker, nameof(RevisionsTable.TransactionMarker), ref tvr);

            return result;
        }

        public static unsafe Document ParseRawDataSectionRevisionWithValidation(JsonOperationContext context, ref TableValueReader tvr, int expectedSize, out long etag)
        {
            var ptr = tvr.Read((int)RevisionsTable.Document, out var size);
            if (size > expectedSize || size <= 0)
                throw new ArgumentException("Data size is invalid, possible corruption when parsing BlittableJsonReaderObject", nameof(size));

            var result = new Document
            {
                StorageId = tvr.Id,
                LowerId = TableValueToString(context, (int)RevisionsTable.LowerId, ref tvr),
                Id = TableValueToId(context, (int)RevisionsTable.Id, ref tvr),
                Etag = etag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr),
                Data = new BlittableJsonReaderObject(ptr, size, context),
                LastModified = TableValueToDateTime((int)RevisionsTable.LastModified, ref tvr),
                Flags = TableValueToFlags((int)RevisionsTable.Flags, ref tvr),
                TransactionMarker = *(short*)tvr.Read((int)RevisionsTable.TransactionMarker, out size),
                ChangeVector = TableValueToChangeVector(context, (int)RevisionsTable.ChangeVector, ref tvr)
            };

            if (size != sizeof(short))
                throw new ArgumentException("TransactionMarker size is invalid, possible corruption when parsing BlittableJsonReaderObject", nameof(size));

            return result;
        }

        private unsafe ByteStringContext.ExternalScope GetResolvedSlice(DocumentsOperationContext context, DateTime date, out Slice slice)
        {
            var size = sizeof(int) + sizeof(long);
            var mem = context.GetMemory(size);
            var flag = (int)DocumentFlags.Resolved;
            Memory.Copy(mem.Address, (byte*)&flag, sizeof(int));
            var ticks = Bits.SwapBytes(date.Ticks);
            Memory.Copy(mem.Address + sizeof(int), (byte*)&ticks, sizeof(long));
            return Slice.External(context.Allocator, mem.Address, size, out slice);
        }

        public IEnumerable<Document> GetResolvedDocumentsSince(DocumentsOperationContext context, DateTime since, long take = 1024)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
            using (GetResolvedSlice(context, since, out var slice))
            {
                foreach (var item in table.SeekForwardFrom(RevisionsSchema.Indexes[ResolvedFlagByEtagSlice], slice, 0))
                {
                    if (take == 0)
                    {
                        yield break;
                    }
                    take--;
                    yield return TableValueToRevision(context, ref item.Result.Reader);
                }
            }
        }

        public long GetNumberOfRevisionDocuments(DocumentsOperationContext context)
        {
            var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);
            return table.GetNumberOfEntriesFor(RevisionsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice]);
        }

    }
}
