using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Documents.Revisions
{
    public class RevisionsKeyCleaner : ITombstoneAware
    {
        private readonly DocumentDatabase _database;
        private readonly CancellationToken _cancellationToken;
        private readonly RavenLogger _logger;

        public RevisionsKeyCleaner(DocumentDatabase database, CancellationToken databaseShutdown)
        {
            _database = database;
            _cancellationToken = databaseShutdown;
            _logger = RavenLogManager.Instance.GetLoggerForDatabase<RevisionsKeyCleaner>(_database);
            _database.TombstoneCleaner.Subscribe(this);
        }

        public async Task Start()
        {
            var lastProgress = string.Empty;
            try
            {
                while (true)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    var batch = new CleanupBatch();
                    await _database.TxMerger.Enqueue(batch);
                    lastProgress = batch.Progress;

                    if (batch.Done)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Revisions key cleanup is done");
                        return;
                    }

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Revisions key cleanup progress: {lastProgress}");

                    await Task.Delay(TimeSpan.FromMilliseconds(250), _cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"An error occurred in the revisions key cleanup, last progress: '{lastProgress}'; it continues on the next database load", e);
            }
        }

        public string TombstoneCleanerIdentifier => "Revisions key cleanup";

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType type,
            Dictionary<string, LastTombstoneInfo> lastProcessedTombstonesInfo = null)
        {
            if (type != ITombstoneAware.TombstoneType.Documents)
                return null;

            if (_database.DocumentsStorage.RevisionsStorage.HashedRevisionPk)
                return null;

            return new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase) { [RevisionsTombstones] = 0 };
        }

        public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
            => new();
    }

    internal sealed class CleanupBatch : DocumentMergedTransactionCommand
    {
        internal static TimeSpan MaxTime = TimeSpan.FromMilliseconds(250);
        private Stopwatch _time;
        private CleanupState _state;
        private Slice _document;
        private int _rows;

        public bool Done { get; private set; }

        public string Progress { get; private set; }

        private enum Scan
        {
            Found,
            BatchLimitReached,
            Completed
        }

        private bool CanContinueBatch(DocumentsOperationContext context) =>
            _time.Elapsed < MaxTime && context.CanContinueTransaction;

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            _time = Stopwatch.StartNew();
            var tx = context.Transaction.InnerTransaction;
            if (CleanupState.TryRead(tx, out _state, out var document) == false)
            {
                _state = new CleanupState
                {
                    LastRawEtag = context.DocumentDatabase.DocumentsStorage.ReadLastEtag(tx)
                };
            }

            using (Slice.From(context.Allocator, document, out _document))
            {
                switch (_state.Step)
                {
                    case CleanupStep.Revisions:
                    case CleanupStep.RevisionAttachments:
                    case CleanupStep.RevisionAttachmentTombstones:
                        if (WalkRevisions(context) == Scan.Completed)
                            EnterRevisionsTombstonesStep(context);
                        break;

                    case CleanupStep.RevisionsTombstones:
                        if (WalkRevisionsTombstones(context) == Scan.Completed)
                            EnterDoneStep();
                        break;

                    case CleanupStep.Done:
                        MarkAllRevisionRowsHashed(context);
                        Done = true;
                        break;
                }

                _state.Write(tx, _document);

                var documentId = _document.Size == 0 ? string.Empty : _document.ToString()[..^1];
                Progress = $"{_state}, document '{documentId}', rows {_rows}, transaction size {tx.LowLevelTransaction.TransactionSize}";
            }

            return _rows;
        }

        private void EnterRevisionsTombstonesStep(DocumentsOperationContext context)
        {
            var schema = context.DocumentDatabase.DocumentsStorage.TombstonesSchema;
            var last = context.Transaction.InnerTransaction.OpenTable(schema, RevisionsTombstonesSlice)?.ReadLast(schema.FixedSizeIndexes[CollectionEtagsSlice]);
            _state = new CleanupState
            {
                LastRawEtag = _state.LastRawEtag,
                Step = CleanupStep.RevisionsTombstones,
                End = last == null ? 0 : TableValueToEtag((int)TombstoneTable.Etag, ref last.Reader)
            };
            _document = Slices.Empty;
        }

        private void EnterDoneStep()
        {
            _state = new CleanupState
            {
                LastRawEtag = _state.LastRawEtag, 
                Step = CleanupStep.Done
            };
        }

        private Scan WalkRevisions(DocumentsOperationContext context)
        {
            while (CanContinueBatch(context))
            {
                if (_state.DocumentInProgress)
                {
                    if (_state.Step == CleanupStep.Revisions)
                    {
                        Scan scan;
                        while ((scan = NextRawRevision(context, out var raw)) == Scan.Found)
                            MoveRevisionToHashedKey(context, raw);

                        if (scan == Scan.BatchLimitReached)
                            return Scan.BatchLimitReached;

                        _state.Step = CleanupStep.RevisionAttachments;
                    }

                    if (MoveRevisionAttachments(context) == Scan.BatchLimitReached)
                        return Scan.BatchLimitReached;

                    _state.Step = CleanupStep.Revisions;
                    _state.End = 0;
                    continue;
                }

                if (MoveToNextDocument(context))
                    continue;

                return Scan.Completed;
            }

            return Scan.BatchLimitReached;
        }

        private bool MoveToNextDocument(DocumentsOperationContext context)
        {
            using (var counts = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice).Iterate(prefetch: false))
            {
                var seekKey = _document.Size == 0 ? Slices.BeforeAllKeys : _document;
                if (counts.Seek(seekKey) == false)
                    return false;

                // Seek lands on the first key >= seekKey; step past the document that was just finished
                if (SliceComparer.Equals(counts.CurrentKey, _document) && counts.MoveNext() == false)
                    return false;

                _state.Cursor = 0;
                _state.End = context.DocumentDatabase.DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                _document = counts.CurrentKey.Clone(context.Allocator);
            }

            _rows++;
            return true;
        }

        private unsafe Scan NextRawRevision(DocumentsOperationContext context, out TableValueReader raw)
        {
            raw = default;
            var schema = context.DocumentDatabase.DocumentsStorage.RevisionsStorage.RevisionsSchema;
            var revisions = new Table(schema, context.Transaction.InnerTransaction);

            using (Slice.External(context.Allocator, _document, _document.Size - 1, out var lowerId))
            using (RevisionsStorage.GetKeyWithEtag(context, lowerId, _state.Cursor + 1, out var start))
            {
                foreach (var read in revisions.SeekForwardFromPrefix(schema.Indexes[IdAndEtagSlice], start, _document, skip: 0))
                {
                    var tvr = read.Result.Reader;
                    var etag = TableValueToEtag((int)RevisionsTable.Etag, ref tvr);
                    if (etag > _state.End)
                        return Scan.Completed;
                    if (CanContinueBatch(context) == false)
                        return Scan.BatchLimitReached;

                    _state.Cursor = etag;
                    _rows++;

                    tvr.Read((int)RevisionsTable.RevisionPk, out var keySize);
                    if (keySize == RevisionsStorage.RevisionKeySize)
                        return Scan.Completed; // hashed keys came with 7.2.5, which writes no 6.2 key: every later etag is hashed too

                    raw = tvr;
                    return Scan.Found;
                }
            }

            return Scan.Completed;
        }

        private unsafe void MoveRevisionToHashedKey(DocumentsOperationContext context, TableValueReader stored)
        {
            var tx = context.Transaction.InnerTransaction;
            var storage = context.DocumentDatabase.DocumentsStorage;
            var revisions = new Table(storage.RevisionsStorage.RevisionsSchema, tx);

            using var row = RevisionsStorage.TableValueToRevision(context, ref stored);
            var deletedEtag = TableValueToLong((int)RevisionsTable.DeletedEtag, ref stored);
            var flags = row.Flags;
            string changeVector = row.ChangeVector;
            var etag = row.Etag;
            var transactionMarker = row.TransactionMarker;

            using (Slice.External(context.Allocator, stored.Read((int)RevisionsTable.RevisionPk, out var keySize), keySize, out var raw))
            using (RevisionsStorage.GetRevisionKeyHashSlice(context.Allocator, raw, out var hash))
            {
                if (revisions.ReadByKey(hash, out var twinRow))
                {
                    using var twin = RevisionsStorage.TableValueToRevision(context, ref twinRow, DocumentFields.LowerId | DocumentFields.ChangeVector);
                    if (twin.LowerId.Equals(row.LowerId) == false)
                        throw new InvalidOperationException($"Revision '{changeVector}' of '{row.Id}' has the hash of revision '{twin.ChangeVector}' of another document");

                    flags |= twin.Flags;
                    if (twin.Flags.Contain(DocumentFlags.Conflicted))
                    {
                        // the twin brought the Conflicted flag: keep its etag, so whoever had not received it still will
                        etag = twin.Etag;
                        transactionMarker = twin.TransactionMarker;
                    }

                    RevisionsStorage.IncrementCountOfRevisions(context, _document, -1); // the twin and this row become one row

                    var twinChangeVector = context.GetChangeVector(twin.ChangeVector);
                    if (twinChangeVector.IsSingle == false)
                        changeVector = new ChangeVector(context.GetChangeVector(changeVector), twinChangeVector.Order);
                }
            }

            using var data = row.Data.Clone(context);
            var owner = storage.RevisionsStorage.EnsureRevisionTableCreated(tx, storage.ExtractCollectionName(context, data));
            using (RevisionsStorage.BuildRevisionKeys(context, context.GetChangeVector(row.ChangeVector), row.Id, out var keys))
            {
                owner.DeleteByKey(keys.Revision.Raw);
                owner.DeleteByKey(keys.Revision.PrefixedHash);
                storage.RevisionsStorage.WriteRevisionTableRecord(context, owner, in keys, new RevisionsTableRow
                {
                    EtagSwapBytes = Bits.SwapBytes(etag),
                    DocumentPtr = data.BasePointer,
                    DocumentSize = data.Size,
                    Flags = flags,
                    DeletedEtagOrMarker = deletedEtag,
                    LastModifiedTicks = row.LastModified.Ticks,
                    TransactionMarker = transactionMarker,
                    ResolvedField = flags.Contain(DocumentFlags.Resolved) ? (int)DocumentFlags.Resolved : 0,
                    FullChangeVector = changeVector
                });
            }
        }

        private Scan MoveRevisionAttachments(DocumentsOperationContext context)
        {
            var tx = context.Transaction.InnerTransaction;
            var storage = context.DocumentDatabase.DocumentsStorage;
            if (_state.Step == CleanupStep.RevisionAttachments)
            {
                if (MoveRevisionAttachmentRows(context, tx.OpenTable(storage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice)) == Scan.BatchLimitReached)
                    return Scan.BatchLimitReached;

                _state.Step = CleanupStep.RevisionAttachmentTombstones;
            }

            return MoveRevisionAttachmentRows(context, tx.OpenTable(storage.TombstonesSchema, AttachmentsTombstonesSlice));
        }

        private Scan MoveRevisionAttachmentRows(DocumentsOperationContext context, Table table)
        {
            if (table == null)
                return Scan.Completed;

            var lowerIdSize = _document.AsReadOnlySpan().IndexOf(AttachmentsStorage.AttachmentKey.RecordSeparator);
            Slice.External(context.Allocator, _document, lowerIdSize + 1, out var documentPrefix);
            var changeVectorStart = documentPrefix.Size + 2;
            if (_document.Size == documentPrefix.Size)
            {
                using (Slice.External(context.Allocator, _document, lowerIdSize, out var lowerId))
                    AttachmentsStorage.AttachmentKey.GetPrefix(context, lowerId, AttachmentType.Revision, Slices.Empty, out _document); // [lowerId][RS]'r'[RS][RS]
            }
            var seekAgain = true;
            while (seekAgain)
            {
                seekAgain = false;
                foreach (var (key, holder) in table.SeekByPrimaryKeyPrefix(documentPrefix, _document, 0))
                {
                    var span = key.AsReadOnlySpan();
                    var changeVectorSize = span.Slice(changeVectorStart).IndexOf(AttachmentsStorage.AttachmentKey.RecordSeparator);
                    if (changeVectorSize <= 0 || changeVectorSize == RevisionsStorage.RevisionKeySize)
                    {
                        // nothing to move, but passing the row takes time too: the next batch continues after it
                        if (CanContinueBatch(context) == false)
                        {
                            _document = key.Clone(context.Allocator);
                            return Scan.BatchLimitReached;
                        }
                        continue;
                    }

                    _document = key.Clone(context.Allocator);
                    _rows++;
                    using (Slice.External(context.Allocator, key.Content, changeVectorStart, changeVectorSize, out var changeVector))
                        MoveRevisionAttachmentToHashedKey(context, table, holder.Reader, key, changeVector);

                    // checked after a move, not before: every call moves at least one row, so a resumed document always moves on
                    if (CanContinueBatch(context) == false)
                        return Scan.BatchLimitReached;

                    seekAgain = true;
                    break; // the table changed: seek again, after this row
                }
            }

            _document = documentPrefix;
            return Scan.Completed;
        }

        private static void MoveRevisionAttachmentToHashedKey(DocumentsOperationContext context, Table table, TableValueReader row, Slice key,
            Slice changeVector)
        {
            var (keyField, revisionVersionField) = RevisionAttachmentFields(table);
            using (RevisionsStorage.GetRevisionKeyHashSlice(context.Allocator, changeVector, out var hash))
            using (AttachmentsStorage.BuildRevisionAttachmentKeyFromComposite(context, new RevisionKey(hash, changeVector), key, out var moved))
            {
                if (table.VerifyKeyExists(moved.HashComposite))
                {
                    table.Delete(row.Id); // a copy: the row under the hashed key names the same stream
                    return;
                }

                using (table.Allocate(row, out TableValueBuilder tvb))
                {
                    tvb.Set(keyField, moved.HashComposite);
                    tvb.Set(revisionVersionField, moved.RevisionVersion); // the change vector in the row's own key ("A:10"), as 6.2 peers hold it
                    table.Delete(row.Id);
                    table.Insert(tvb);
                }
            }
        }

        private static (int KeyField, int RevisionVersionField) RevisionAttachmentFields(Table table)
        {
            if (SliceComparer.Equals(table.Name, AttachmentsMetadataSlice))
                return ((int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, (int)AttachmentsTable.RevisionVersion);
            if (SliceComparer.Equals(table.Name, AttachmentsTombstonesSlice))
                return ((int)TombstoneTable.LowerId, (int)TombstoneTable.RevisionVersion);

            throw new InvalidOperationException($"'{table.Name}' holds no revision attachments");
        }

        private Scan WalkRevisionsTombstones(DocumentsOperationContext context)
        {
            Scan scan;
            while ((scan = NextTombstone(context, out var tombstone)) == Scan.Found)
                Clean(context, tombstone);

            return scan;
        }

        private Scan NextTombstone(DocumentsOperationContext context, out TableValueReader next)
        {
            next = default;
            if (_state.Cursor == _state.End)
                return Scan.Completed;

            var schema = context.DocumentDatabase.DocumentsStorage.TombstonesSchema;
            var tombstones = context.Transaction.InnerTransaction.OpenTable(schema, RevisionsTombstonesSlice);

            foreach (var read in tombstones.SeekForwardFrom(schema.FixedSizeIndexes[CollectionEtagsSlice], _state.Cursor + 1, skip: 0))
            {
                var tombstone = read.Reader;
                var etag = TableValueToEtag((int)TombstoneTable.Etag, ref tombstone);
                if (etag > _state.End)
                    break;
                if (CanContinueBatch(context) == false)
                    return Scan.BatchLimitReached;

                _state.Cursor = etag;
                _rows++;
                next = tombstone;
                return Scan.Found;
            }

            return Scan.Completed;
        }

        private void Clean(DocumentsOperationContext context, TableValueReader stored)
        {
            var tx = context.Transaction.InnerTransaction;
            var storage = context.DocumentDatabase.DocumentsStorage;

            var holder = new Table.TableValueHolder { Reader = stored };
            using (TableValueReaderUtil.CloneTableValueReader(context, holder))
            {
                var row = holder.Reader;
                using var tombstone = TableValueToTombstone(context, ref row);
                using var keys = RevisionsStorage.BuildRevisionTombstoneKeyForExternal(context, tombstone, out var key);
                if (key.DocIdSlice.Size == 0)
                    return; // a pre-6.0 tombstone without a document id

                var lowerId = key.DocIdSlice;
                var changeVector = key.RevisionVersion;

                if (tombstone.RevisionVersion == null)
                    MoveTombstoneToHashedKey(context, stored.Id, row, tombstone);

                using (RevisionsStorage.GetRevisionKeyHashSlice(context.Allocator, changeVector, out var hash))
                {
                    var revisions = new Table(storage.RevisionsStorage.RevisionsSchema, tx);
                    if (revisions.ReadByKey(hash, out var revisionRow))
                    {
                        using var revision = RevisionsStorage.TableValueToRevision(context, ref revisionRow, DocumentFields.LowerId | DocumentFields.Data);
                        if (revision.LowerId.AsReadOnlySpan().SequenceEqual(lowerId.AsReadOnlySpan()) == false ||
                            tombstone.Flags.Contain(DocumentFlags.Artificial | DocumentFlags.FromResharding) ||
                            revision.Flags.Contain(DocumentFlags.ForceCreated))
                            return;

                        var owner = storage.RevisionsStorage.EnsureRevisionTableCreated(tx, storage.ExtractCollectionName(context, revision.Data));
                        owner.Delete(revision.StorageId);

                        using (RevisionsStorage.GetKeyPrefix(context.Allocator, lowerId, out var countKey))
                            RevisionsStorage.IncrementCountOfRevisions(context, countKey, -1);
                    }

                    using (AttachmentsStorage.BuildRevisionAttachmentPrefix(context, new RevisionKey(hash, changeVector), lowerId, out var prefixes))
                    {
                        DeleteRevisionAttachments(context, prefixes.HashComposite);
                        DeleteRevisionAttachments(context, prefixes.RawComposite);
                        MoveAttachmentTombstonesOfRevision(context, prefixes.RawComposite, changeVector);
                    }
                }
            }
        }

        private void MoveTombstoneToHashedKey(DocumentsOperationContext context, long id, TableValueReader row, Tombstone tombstone)
        {
            var storage = context.DocumentDatabase.DocumentsStorage;
            var table = context.Transaction.InnerTransaction.OpenTable(storage.TombstonesSchema, RevisionsTombstonesSlice);
            using (storage.RevisionsStorage.BuildRevisionTombstoneKeyFromExternal(context, tombstone.LowerId, tombstone.Collection, out var received))
            {
                var hashedKey = received.HashComposite;
                var changeVector = received.RevisionVersion;
                var flags = tombstone.Flags;
                var lastModified = tombstone.LastModified.Ticks;
                var source = row;
                if (table.ReadByKey(hashedKey, out var existing))
                {
                    flags |= TableValueToFlags((int)TombstoneTable.Flags, ref existing);
                    lastModified = Math.Max(lastModified, TableValueToLong((int)TombstoneTable.LastModified, ref existing));
                    if (TableValueToEtag((int)TombstoneTable.Etag, ref existing) < tombstone.Etag)
                        source = existing;
                }

                using (table.Allocate(source, out TableValueBuilder tvb))
                {
                    tvb.Set((int)TombstoneTable.LowerId, hashedKey);
                    tvb.Set((int)TombstoneTable.Flags, (int)flags);
                    tvb.Set((int)TombstoneTable.LastModified, lastModified);
                    tvb.Set((int)TombstoneTable.RevisionVersion, changeVector); // a 6.2 row has no such column yet: appended
                    table.Delete(id);
                    table.DeleteByKey(hashedKey);
                    table.Insert(tvb);
                }
            }
        }

        private void MoveAttachmentTombstonesOfRevision(DocumentsOperationContext context, Slice rawPrefix, Slice changeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(context.DocumentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            if (table == null)
                return;

            // each move deletes the row under the raw key, so the next seek finds the next one
            while (table.SeekOnePrimaryKeyPrefix(rawPrefix, out var row))
            {
                _rows++;
                using (TableValueToSlice(context, (int)TombstoneTable.LowerId, ref row, out var key))
                    MoveRevisionAttachmentToHashedKey(context, table, row, key, changeVector);
            }
        }

        private static void DeleteRevisionAttachments(DocumentsOperationContext context, Slice revisionPrefix)
        {
            var attachments = context.Transaction.InnerTransaction.OpenTable(context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice);
            if (attachments == null)
                return;

            while (attachments.SeekOnePrimaryKeyPrefix(revisionPrefix, out var row))
            {
                using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref row, out var contentHash))
                    context.Transaction.CheckIfShouldDeleteAttachmentStream(contentHash);
                attachments.Delete(row.Id);
            }
        }

        private static void MarkAllRevisionRowsHashed(DocumentsOperationContext context)
        {
            var tx = context.Transaction.InnerTransaction;
            RevisionsStorage.WriteAllRevisionRowsHashed(tx);

            var database = context.DocumentDatabase;
            var llt = tx.LowLevelTransaction;
            llt.OnDispose += _ =>
            {
                if (llt.Committed == false)
                    return; // still subscribed: the revision tombstones stay until the next load runs the cleanup again

                database.DocumentsStorage.RevisionsStorage.OnAllRevisionRowsHashed();
                database.TombstoneCleaner.Unsubscribe(database.RevisionsKeyCleaner);
            };
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            => throw new NotImplementedException();
    }

    internal enum CleanupStep : long
    {
        Revisions,
        RevisionAttachments,
        RevisionAttachmentTombstones,
        RevisionsTombstones,
        Done
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct CleanupState
    {
        public long LastRawEtag;
        public CleanupStep Step;
        public long Cursor;
        public long End;

        public readonly bool DocumentInProgress => End != 0;

        internal static readonly Slice StateSlice;

        static CleanupState()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
                Slice.From(ctx, "RevisionsKeyCleanerState", ByteStringType.Immutable, out StateSlice);
        }

        internal static unsafe bool TryRead(Transaction tx, out CleanupState state, out ReadOnlySpan<byte> document)
        {
            state = default;
            document = Array.Empty<byte>();
            if (tx.ReadTree(GlobalTreeSlice).TryRead(StateSlice, out var reader) == false)
                return false;

            state = Unsafe.ReadUnaligned<CleanupState>(reader.Base);
            document = new ReadOnlySpan<byte>(reader.Base + sizeof(CleanupState), reader.Length - sizeof(CleanupState));
            return true;
        }

        internal readonly unsafe void Write(Transaction tx, Slice document)
        {
            using (tx.ReadTree(GlobalTreeSlice).DirectAdd(StateSlice, sizeof(CleanupState) + document.Size, out var value))
            {
                Unsafe.WriteUnaligned(value, this);
                document.CopyTo(value + sizeof(CleanupState));
            }
        }

        public readonly override string ToString() =>
            $"step {Step}, cursor {Cursor}, end {End}, document in progress {DocumentInProgress}, last raw etag {LastRawEtag}";
    }

    public partial class RevisionsStorage
    {
        private bool _storageHashed;

        internal bool HashedRevisionPk => _database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk || _storageHashed;

        private void MarkStorageHashed(Transaction tx)
        {
            if (_documentsStorage.Environment.IsNew)
                WriteAllRevisionRowsHashed(tx);

            _storageHashed = AllRevisionRowsHashed(tx);
        }

        internal void OnAllRevisionRowsHashed() => _storageHashed = true;

        internal static void WriteAllRevisionRowsHashed(Transaction tx)
            => tx.ReadTree(GlobalTreeSlice).Add(Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk, bool.TrueString);

        internal static bool AllRevisionRowsHashed(Transaction tx)
            => tx.ReadTree(GlobalTreeSlice).TryRead(Constants.DatabaseRecord.SupportedFeatures.HashedRevisionPk, out _);

        private bool CleanupHasNotPassed(DocumentsOperationContext context, Table table, Slice lowerIdPrefix)
        {
            if (HashedRevisionPk)
                return false;

            var tx = context.Transaction.InnerTransaction;
            if (CleanupState.TryRead(tx, out var cleanup, out var walked) == false)
                return true;

            if (cleanup.Step >= CleanupStep.RevisionsTombstones || cleanup.LastRawEtag == 0)
                return false;

            if (walked.IsEmpty == false)
            {
                var order = lowerIdPrefix.AsReadOnlySpan().SequenceCompareTo(walked);
                if (order < 0 || order == 0 && cleanup.DocumentInProgress == false)
                    return false;
            }

            foreach (var read in table.SeekForwardFromPrefix(RevisionsSchema.Indexes[IdAndEtagSlice], lowerIdPrefix, lowerIdPrefix, skip: 0))
            {
                var tvr = read.Result.Reader;
                return TableValueToEtag((int)RevisionsTable.Etag, ref tvr) <= cleanup.LastRawEtag;
            }

            return false;
        }
    }
}
