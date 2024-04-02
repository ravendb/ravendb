using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Voron;
using Voron.Data.Tables;
using Table = Voron.Data.Tables.Table;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class DocumentDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/documents/huge", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task HugeDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                writer.WriteStartArray();

                var isFirst = true;

                foreach (var pair in context.DocumentDatabase.HugeDocuments.GetHugeDocuments())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName("Id");
                    writer.WriteString(pair.Key.Item1);

                    writer.WriteComma();

                    writer.WritePropertyName("Size");
                    writer.WriteInteger(pair.Value);

                    writer.WriteComma();

                    writer.WritePropertyName("LastAccess");
                    writer.WriteString(pair.Key.Item2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/debug/documents/fix-collection-discrepancy", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task FixCollectionDiscrepancy()
        {
            var id = GetStringQueryString("id", required: false);
            var collection = GetStringQueryString("collection", required: false);

            if (string.IsNullOrEmpty(id) && string.IsNullOrEmpty(collection))
                throw new ArgumentException($"Missing {nameof(id)} or {nameof(collection)} property");

            if (string.IsNullOrEmpty(collection) == false)
            {
                var startEtag = 0L;

                using (var token = CreateHttpRequestBoundOperationToken())
                {
                    while (token.Token.IsCancellationRequested == false)
                    {
                        (HashSet<string> ids, long lastEtag) = GetDocumentIdsToFix(collection, startEtag, token);

                        if (ids.Count == 0)
                            break;

                        startEtag = lastEtag;

                        var cmd = new FixCollectionDiscrepancyCommand(Database, ids);
                        await Database.TxMerger.Enqueue(cmd);
                    }
                }

                return;
            }

            var command = new FixCollectionDiscrepancyCommand(Database, new HashSet<string> { id });
            await Database.TxMerger.Enqueue(command);
        }

        private (HashSet<string> Ids, long LastEtag) GetDocumentIdsToFix(string collection, long startEtag, OperationCancelToken token)
        {
            var ids = new HashSet<string>();
            long lastEtag = 0;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var docsForCollection = Database.DocumentsStorage.GetDocumentsFrom(context, collection, startEtag, 0, int.MaxValue, DocumentFields.Id);
                var collectionName = Database.DocumentsStorage.ExtractCollectionName(context, collection);

                foreach (var document in docsForCollection)
                {
                    token.ThrowIfCancellationRequested();

                    using (document)
                    {
                        var documentId = document.Id.ToString();
                        var seriesNames = Database.DocumentsStorage.TimeSeriesStorage.Stats.GetTimeSeriesNamesForDocumentOriginalCasing(context, documentId).ToList();

                        foreach (var name in seriesNames)
                        {
                            token.ThrowIfCancellationRequested();

                            var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, documentId, name, from: DateTime.MinValue, to: DateTime.MaxValue);
                            reader.Init();

                            var table = Database.DocumentsStorage.TimeSeriesStorage.GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);
                            foreach (var _ in reader.GetSegments())
                            {
                                if (table.IsOwned(reader.SegmentStorageId) == false)
                                {
                                    ids.Add(documentId);
                                    break;
                                }
                            }

                            var statsTable = Database.DocumentsStorage.TimeSeriesStorage.Stats.GetOrCreateTable(context.Transaction.InnerTransaction, collectionName);
                            using (reader.ReadKey(out var key))
                            using (Slice.External(context.Allocator, key, key.Size - sizeof(long) - 1, out var statsKey))
                            {
                                if (statsTable.ReadByKey(statsKey, out var tvr) && statsTable.IsOwned(tvr.Id) == false)
                                {
                                    ids.Add(documentId);
                                    break;
                                }
                            }
                        }

                        //TODO: add attachments and counters

                        lastEtag = document.Etag;

                        if (ids.Count > 1024)
                            break;
                    }
                }
            }

            return (ids, lastEtag);
        }

        private class FixCollectionDiscrepancyCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly HashSet<string> _ids;

            public FixCollectionDiscrepancyCommand(DocumentDatabase database, HashSet<string> ids)
            {
                _database = database;
                _ids = ids;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var count = 0;
                foreach (var id in _ids)
                {
                    if (UpdateDocument(context, id))
                        count++;
                }

                return count;
            }

            private bool UpdateDocument(DocumentsOperationContext context, string id)
            {
                var document = context.DocumentDatabase.DocumentsStorage.Get(context, id, fields: DocumentFields.Data);
                if (document == null)
                    return false;

                var collections = _database.DocumentsStorage.ReadCollections(context.Transaction.InnerTransaction).Values.ToList();
                var documentCollectionName = _database.DocumentsStorage.ExtractCollectionName(context, document.Data);
                var seriesNames = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetTimeSeriesNamesForDocumentOriginalCasing(context, id).ToList();

                var hasUpdates = false;
                hasUpdates |= UpdateTimeSeriesSegments(context, id, documentCollectionName, seriesNames, collections);
                hasUpdates |= UpdateTimeSeriesStats(context, id, documentCollectionName, seriesNames, collections);

                //TODO: add attachments and counters

                return hasUpdates;
            }

            private bool UpdateTimeSeriesSegments(DocumentsOperationContext context, string documentId, CollectionName documentCollectionName, List<string> seriesNames, List<CollectionName> collections)
            {
                using var _ = Slice.From(context.Allocator, documentCollectionName.Name, out var collectionSlice);
                var table = _database.DocumentsStorage.TimeSeriesStorage.GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, documentCollectionName);

                var hasUpdates = false;

                foreach (var name in seriesNames)
                {
                    while (true)
                    {
                        if (TryMoveSegments(context, documentId, name, table, collections, collectionSlice) == false)
                            break;

                        hasUpdates = true;
                    }
                }

                return hasUpdates;
            }

            private bool UpdateTimeSeriesStats(DocumentsOperationContext context, string documentId, CollectionName collectionName, List<string> seriesNames, List<CollectionName> collections)
            {
                var hasUpdates = false;
                var table = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetOrCreateTable(context.Transaction.InnerTransaction, collectionName);

                foreach (var name in seriesNames)
                {
                    var reader = _database.DocumentsStorage.TimeSeriesStorage.GetReader(context, documentId, name, from: DateTime.MinValue, to: DateTime.MaxValue);
                    reader.Init();

                    using (reader.ReadKey(out var key))
                    using (Slice.External(context.Allocator, key, key.Size - sizeof(long) - 1, out var statsKey))
                    {
                        if (table.ReadByKey(statsKey, out var tvr) == false)
                            continue;

                        if (table.IsOwned(tvr.Id))
                            continue;

                        hasUpdates = true;

                        (long Count, DateTime Start, DateTime End, Slice PolicyName, Slice OriginalName) stats = default;
                        foreach (var collection in collections)
                        {
                            var oldTable = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetOrCreateTable(context.Transaction.InnerTransaction, collection);
                            if (oldTable.IsOwned(tvr.Id))
                            {
                                // deleting old info
                                stats = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetFullStats(context, statsKey);
                                _database.DocumentsStorage.TimeSeriesStorage.Stats.DeleteStats(context, collection, statsKey);
                                break;
                            }
                        }

                        using (table.Allocate(out var tvb))
                        {
                            tvb.Add(statsKey);
                            tvb.Add(stats.PolicyName);
                            tvb.Add(Bits.SwapBytes(stats.Start.Ticks));
                            tvb.Add(stats.End);
                            tvb.Add(stats.Count);
                            tvb.Add(stats.OriginalName);

                            table.Insert(tvb);
                        }
                    }
                }

                return hasUpdates;
            }

            private unsafe bool TryMoveSegments(DocumentsOperationContext context, string documentId, string name, Table table, List<CollectionName> collections, Slice collectionSlice)
            {
                var reader = _database.DocumentsStorage.TimeSeriesStorage.GetReader(context, documentId, name, from: DateTime.MinValue, to: DateTime.MaxValue);

                foreach (var seg in reader.GetSegments())
                {
                    if (table.IsOwned(reader.SegmentStorageId))
                        continue;

                    using (reader.ReadKey(out var key))
                    {
                        var info = reader.GetSegmentInfo();
                        var mem = seg.Clone(context, out var cloned);

                        foreach (var collection in collections)
                        {
                            var oldTable = _database.DocumentsStorage.TimeSeriesStorage.GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collection);
                            if (oldTable.IsOwned(reader.SegmentStorageId))
                            {
                                // deleting old info
                                oldTable.DeleteByKey(key);
                                break;
                            }
                        }

                        using (Slice.From(context.Allocator, info.ChangeVector, out Slice cv))
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(key);
                            tvb.Add(Bits.SwapBytes(info.Etag));
                            tvb.Add(cv);
                            tvb.Add(cloned.Ptr, cloned.NumberOfBytes);
                            tvb.Add(collectionSlice);
                            tvb.Add(info.TransactionMarker);

                            table.Insert(tvb);
                        }

                        context.ReturnMemory(mem);

                        return true;
                    }
                }

                return false;
            }


            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
            {
                throw new NotImplementedException();
            }
        }
    }
}
