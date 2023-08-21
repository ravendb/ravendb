using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide;
using Raven.Server.Monitoring.Snmp.Objects.Cluster;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Expiration
{
    public sealed unsafe class ExpirationStorage
    {
        private const string DocumentsByExpiration = "DocumentsByExpiration";
        private const string DocumentsByRefresh = "DocumentsByRefresh";

        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public ExpirationStorage(DocumentDatabase database, Transaction tx)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<ExpirationStorage>(database.Name);

            tx.CreateTree(DocumentsByExpiration);
            tx.CreateTree(DocumentsByRefresh);
        }

        public void Put(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject metadata)
        {
            var hasExpirationDate = metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate);
            var hasRefreshDate = metadata.TryGet(Constants.Documents.Metadata.Refresh, out string refreshDate);

            if (hasExpirationDate == false && hasRefreshDate == false)
                return;

            if (hasExpirationDate)
                PutInternal(context, lowerId, expirationDate, DocumentsByExpiration);

            if (hasRefreshDate)
                PutInternal(context, lowerId, refreshDate, DocumentsByRefresh);
        }

        private void PutInternal(DocumentsOperationContext context, Slice lowerId, string expirationDate, string treeName)
        {
            if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date) == false)
                ThrowWrongExpirationDateFormat(lowerId, expirationDate);

            // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
            // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
            // and we'll use the normal cleanup routine to clean things up later.

            var expiry = date.ToUniversalTime();
            var ticksBigEndian = Bits.SwapBytes(expiry.Ticks);

            var tree = context.Transaction.InnerTransaction.ReadTree(treeName);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
                tree.MultiAdd(ticksSlice, lowerId);
        }

        [DoesNotReturn]
        private void ThrowWrongExpirationDateFormat(Slice lowerId, string expirationDate)
        {
            throw new InvalidOperationException(
                $"The expiration date format for document '{lowerId}' is not valid: '{expirationDate}'. Use the following format: {_database.Time.GetUtcNow():O}");
        }

        public record ExpiredDocumentsOptions
        {
            public DocumentsOperationContext Context;
            public DateTime CurrentTime;
            public DatabaseTopology DatabaseTopology;
            public string NodeTag;
            public long AmountToTake;

            public ExpiredDocumentsOptions(DocumentsOperationContext context, DateTime currentTime, DatabaseTopology databaseTopology, string nodeTag, long amountToTake) =>
                (Context, CurrentTime, DatabaseTopology, NodeTag , AmountToTake)
                = (context, currentTime, databaseTopology, nodeTag, amountToTake);
        }

        public Dictionary<Slice, List<(Slice LowerId, string Id)>> GetExpiredDocuments(ExpiredDocumentsOptions options, out Stopwatch duration, CancellationToken cancellationToken)
        {
            return GetDocuments(options, DocumentsByExpiration, Constants.Documents.Metadata.Expires, out duration, cancellationToken);
        }

        public Dictionary<Slice, List<(Slice LowerId, string Id)>> GetDocumentsToRefresh(ExpiredDocumentsOptions options, out Stopwatch duration, CancellationToken cancellationToken)
        {
            return GetDocuments(options, DocumentsByRefresh, Constants.Documents.Metadata.Refresh, out duration, cancellationToken);
        }

        private Dictionary<Slice, List<(Slice LowerId, string Id)>> GetDocuments(ExpiredDocumentsOptions options, string treeName, string metadataPropertyToCheck, out Stopwatch duration, CancellationToken cancellationToken)
        {
            var count = 0;
            var currentTicks = options.CurrentTime.Ticks;

            var expirationTree = options.Context.Transaction.InnerTransaction.ReadTree(treeName);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                {
                    duration = null;
                    return null;
                }

                var expired = new Dictionary<Slice, List<(Slice LowerId, string Id)>>();
                duration = Stopwatch.StartNew();
                
                do
                {
                    var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    if (entryTicks > currentTicks)
                        break;

                    var ticksAsSlice = it.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                    var expiredDocs = new List<(Slice LowerId, string Id)>();

                    using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                    {
                        if (multiIt.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                if (cancellationToken.IsCancellationRequested)
                                    return expired;

                                var clonedId = multiIt.CurrentKey.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                                try
                                {
                                    using (var document = _database.DocumentsStorage.Get(options.Context, clonedId,
                                               DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector))
                                    {
                                        if (document == null ||
                                            document.TryGetMetadata(out var metadata) == false ||
                                            BackgroundWorkHelper.HasPassed(metadata,  options.CurrentTime, metadataPropertyToCheck) == false)
                                        {
                                            expiredDocs.Add((clonedId, null));
                                            continue;
                                        }

                                        if (BackgroundWorkHelper.CheckIfNodeIsFirstInTopology(options) == false)
                                            break;
                                        
                                        expiredDocs.Add((clonedId, document.Id));
                                    }
                                }
                                catch (DocumentConflictException)
                                {
                                    if (BackgroundWorkHelper.CheckIfNodeIsFirstInTopology(options) == false)
                                        break;

                                    var (allExpired, id) = GetConflictedExpiration(options.Context, options.CurrentTime, clonedId);

                                    if (allExpired)
                                    {
                                        expiredDocs.Add((clonedId, id));
                                    }
                                }
                            } while (multiIt.MoveNext() && expiredDocs.Count + count < options.AmountToTake);
                        }
                    }

                    count += expiredDocs.Count;
                    if (expiredDocs.Count > 0)
                        expired.Add(ticksAsSlice, expiredDocs);

                } while (it.MoveNext() && count < options.AmountToTake);

                return expired;
            }
        }

        private (bool AllExpired, string Id) GetConflictedExpiration(DocumentsOperationContext context, DateTime currentTime, Slice clonedId)
        {
            string id = null;
            var allExpired = true;
            var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, clonedId);
            if (conflicts.Count > 0)
            {
                foreach (var conflict in conflicts)
                {
                    using (conflict)
                    {
                        id = conflict.Id;

                        if (BackgroundWorkHelper.HasPassed(conflict.Doc, currentTime,Constants.Documents.Metadata.Expires))
                            continue;

                        allExpired = false;
                        break;
                    }
                }
            }

            return (allExpired, id);
        }


        public int DeleteDocumentsExpiration(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> expired, DateTime currentTime)
        {
            var deletionCount = 0;
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByExpiration);

            foreach (var pair in expired)
            {
                foreach (var ids in pair.Value)
                {
                    if (ids.Id != null)
                    {
                        try
                        {
                            using (var doc = _database.DocumentsStorage.Get(context, ids.LowerId, DocumentFields.Data, throwOnConflict: true))
                            {
                                if (doc != null && doc.TryGetMetadata(out var metadata))
                                {
                                    if (BackgroundWorkHelper.HasPassed(metadata, currentTime, Constants.Documents.Metadata.Expires))
                                    {
                                        _database.DocumentsStorage.Delete(context, ids.LowerId, ids.Id, expectedChangeVector: null);
                                    }
                                }
                            }
                        }
                        catch (DocumentConflictException)
                        {
                            if (GetConflictedExpiration(context, currentTime, ids.LowerId).AllExpired)
                                _database.DocumentsStorage.Delete(context, ids.LowerId, ids.Id, expectedChangeVector: null);
                        }

                        deletionCount++;
                    }

                    expirationTree.MultiDelete(pair.Key, ids.LowerId);
                }
            }

            return deletionCount;
        }

        public int RefreshDocuments(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> expired, DateTime currentTime)
        {
            var refreshCount = 0;
            var refreshTree = context.Transaction.InnerTransaction.ReadTree(DocumentsByRefresh);

            foreach (var pair in expired)
            {
                foreach (var ids in pair.Value)
                {
                    if (ids.Id != null)
                    {
                        using (var doc = _database.DocumentsStorage.Get(context, ids.LowerId, throwOnConflict: false))
                        {
                            if (doc != null && doc.TryGetMetadata(out var metadata))
                            {
                                if (BackgroundWorkHelper.HasPassed(metadata,  currentTime, Constants.Documents.Metadata.Refresh))
                                {
                                    // remove the @refresh tag
                                    metadata.Modifications = new Sparrow.Json.Parsing.DynamicJsonValue(metadata);
                                    metadata.Modifications.Remove(Constants.Documents.Metadata.Refresh);

                                    using (var updated = context.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                                    {
                                        try
                                        {
                                            _database.DocumentsStorage.Put(context, doc.Id, doc.ChangeVector, updated, flags: doc.Flags.Strip(DocumentFlags.FromClusterTransaction));
                                        }
                                        catch (ConcurrencyException)
                                        {
                                            // This is expected and safe to ignore
                                            // It can happen if there is a mismatch with the Cluster-Transaction-Index, which will
                                            // sort itself out when the cluster & database will be in sync again
                                        }
                                        catch (DocumentConflictException)
                                        {
                                            // no good way to handle this, we'll wait to resolve
                                            // the issue when the conflict is resolved
                                        }
                                    }
                                }
                            }
                        }

                        refreshCount++;
                    }

                    refreshTree.MultiDelete(pair.Key, ids.LowerId);
                }
            }

            return refreshCount;
        }
    }
}
