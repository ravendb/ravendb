using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Documents.Expiration
{
    public unsafe class ExpirationStorage
    {
        private const string DocumentsByExpiration = "DocumentsByExpiration";

        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public ExpirationStorage(DocumentDatabase database, Transaction tx)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<ExpirationStorage>(database.Name);

            tx.CreateTree(DocumentsByExpiration);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document)
        {
            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate) == false)
                return;

            PutInternal(context, lowerId, document);
        }

        private void PutInternal(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document)
        {
            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate) == false)
                return;

            if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date) == false)
                throw new InvalidOperationException($"The expiration date format is not valid: '{expirationDate}'. Use the following format: {_database.Time.GetUtcNow():O}");

            // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
            // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
            // and we'll use the normal cleanup routine to clean things up later.

            var ticksBigEndian = Bits.SwapBytes(date.Ticks);

            var tree = context.Transaction.InnerTransaction.ReadTree(DocumentsByExpiration);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
                tree.MultiAdd(ticksSlice, lowerId);
        }

        public Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>> GetExpiredDocuments(DocumentsOperationContext context, 
            DateTime currentTime, CancellationToken cancellationToken, out Stopwatch duration)
        {
            var currentTicks = currentTime.Ticks;

            var expirationTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByExpiration);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                {
                    duration = null;
                    return null;
                }

                var expired = new Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>>();
                duration = Stopwatch.StartNew();

                do
                {
                    var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    if (entryTicks > currentTicks)
                        break;

                    var ticksAsSlice = it.CurrentKey.Clone(context.Transaction.InnerTransaction.Allocator);

                    var expiredDocs = new List<(Slice LowerId, LazyStringValue Id)>();

                    expired.Add(ticksAsSlice, expiredDocs);

                    using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                    {
                        if (multiIt.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                if (cancellationToken.IsCancellationRequested)
                                    return expired;

                                var clonedId = multiIt.CurrentKey.Clone(context.Transaction.InnerTransaction.Allocator);

                                try
                                {
                                    var document = _database.DocumentsStorage.Get(context, clonedId);
                                    if (document == null)
                                    {
                                        expiredDocs.Add((clonedId, null));
                                        continue;
                                    }

                                    if (HasExpired(document.Data, currentTime) == false)
                                        continue;

                                    expiredDocs.Add((clonedId, document.Id));
                                }
                                catch (DocumentConflictException)
                                {
                                    LazyStringValue id = null;
                                    var allExpired = true;
                                    var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, clonedId);
                                    if (conflicts.Count == 0)
                                        continue;

                                    foreach (var conflict in conflicts)
                                    {
                                        id = conflict.Id;

                                        if (HasExpired(conflict.Doc, currentTime))
                                            continue;

                                        allExpired = false;
                                        break;
                                    }

                                    if (allExpired)
                                        expiredDocs.Add((clonedId, id));
                                }
                            } while (multiIt.MoveNext());
                        }
                    }
                } while (it.MoveNext());

                return expired;
            }
        }

        private static bool HasExpired(BlittableJsonReaderObject data, DateTime currentTime)
        {
            // Validate that the expiration value in metadata is still the same.
            // We have to check this as the user can update this value.
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate) == false)
                return false;

            if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date) == false)
                return false;

            if (currentTime < date)
                return false;

            return true;
        }

        public int DeleteExpiredDocuments(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>> _expired)
        {
            var deletionCount = 0;
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByExpiration);

            foreach (var pair in _expired)
            {
                foreach (var ids in pair.Value)
                {
                    if (ids.Id != null)
                    {
                        var deleted = _database.DocumentsStorage.Delete(context, ids.LowerId, ids.Id, expectedChangeVector: null);

                        if (_logger.IsInfoEnabled && deleted == null)
                            _logger.Info($"Tried to delete expired document '{ids.Id}' but document was not found.");

                        deletionCount++;
                    }

                    expirationTree.MultiDelete(pair.Key, ids.LowerId);
                }
            }

            return deletionCount;
        }
    }
}
