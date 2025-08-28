using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Schemas;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.CountersStorage;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Documents;

public class CountersRepairTask
{
    private readonly DocumentDatabase _database;
    private readonly CancellationToken _cancellationToken;
    private readonly Logger _logger;

    public static string Completed = string.Empty;

    public CountersRepairTask(DocumentDatabase database, CancellationToken databaseShutdown)
    {
        _database = database;
        _cancellationToken = databaseShutdown;
        _logger = LoggingSource.Instance.GetLogger<CountersRepairTask>(_database.Name);
    }

    public async Task Start(string lastProcessedKey)
    {
        const int maxNumberOfDocsToFixInSingleTx = 1024;
        List<string> docIdsToFix = [];
        StartAfterSliceHolder startAfterSliceHolder = null;
        string lastDocId = null;

        if (lastProcessedKey != null)
        {
            startAfterSliceHolder = new StartAfterSliceHolder(lastProcessedKey);
        }

        try
        {
            while (true)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var table = new Table(_database.DocumentsStorage.CountersStorage.CountersSchema, context.Transaction.InnerTransaction);

                    while (true)
                    {
                        bool hasMore = false;

                        foreach (var (_, tvh) in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, startAfter: startAfterSliceHolder?.GetStartAfterSlice(context) ?? Slices.Empty, skip: 0))
                        {
                            _cancellationToken.ThrowIfCancellationRequested();

                            hasMore = true;

                            using (var docId = ExtractDocId(context, ref tvh.Reader))
                            {
                                if (docId != lastDocId)
                                {
                                    lastDocId = docId;

                                    using (var old = startAfterSliceHolder)
                                    {
                                        startAfterSliceHolder = new StartAfterSliceHolder(lastDocId);
                                    }
                                }

                                using (var data = GetCounterValuesData(context, ref tvh.Reader))
                                {
                                    data.TryGet(Values, out BlittableJsonReaderObject counterValues);
                                    data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames);
                                    data.TryGet(DbIds, out BlittableJsonReaderArray dbIds);

                                    bool dbIdsCorruption = false;
                                    foreach (var item in dbIds)
                                    {
                                        var lsv = item as LazyStringValue;
                                        if (IsBase64String(lsv) == false)
                                        {
                                            dbIdsCorruption = true;
                                            break;
                                        }
                                    }

                                    if (dbIdsCorruption == false && counterValues.Count == counterNames.Count)
                                    {
                                        var counterValuesPropertyNames = counterValues.GetSortedPropertyNames();
                                        var counterNamesPropertyNames = counterNames.GetSortedPropertyNames();
                                        if (counterValuesPropertyNames.SequenceEqual(counterNamesPropertyNames))
                                            continue;
                                    }
                                }

                                // Document is corrupted; add to list and break to skip remaining CounterGroups of current document
                                docIdsToFix.Add(lastDocId);

                                break;
                            }
                        }

                        if (docIdsToFix.Count == 0)
                        {
                            MarkAsCompleted();
                            return;
                        }

                        if (docIdsToFix.Count < maxNumberOfDocsToFixInSingleTx && hasMore)
                            continue;

                        await _database.TxMerger.Enqueue(new ExecuteFixCounterGroupsCommand(_database, docIdsToFix, hasMore));
                        docIdsToFix.Clear();

                        if (hasMore)
                            break; // break from inner while loop in order to open a new read tx

                        startAfterSliceHolder?.Dispose();
                        return;
                    }
                }
            }
        }
        catch (Exception e)
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"An Error occured while executing FixCorruptedCountersTask. Last DocId : '{lastDocId}'", e);
            }

        }
        finally
        {
            startAfterSliceHolder?.Dispose();
        }
    }

    public static bool IsBase64String(LazyStringValue lsv)
    {
        foreach (var c in lsv)
        {
            bool validChar =
                c is >= 'A' and <= 'Z' ||
                c is >= 'a' and <= 'z' ||
                c is >= '0' and <= '9' ||
                c == '+' || 
                c == '/' || 
                c == '=';

            if (validChar) 
                continue;
            
            return false;
        }

        return true;
    }

    private void MarkAsCompleted()
    {
        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeCtx))
        using (var tx = writeCtx.OpenWriteTransaction())
        {
            _database.DocumentsStorage.SetLastFixedCounterKey(writeCtx, Completed);
            tx.Commit();
        }
    }

    internal int FixCountersForDocuments(DocumentsOperationContext context, List<string> docIds, bool hasMore)
    {
        var numOfCounterGroupsFixed = 0;
        foreach (var docId in docIds)
        {
            numOfCounterGroupsFixed += FixCountersForDocument(context, docId);
            context.DocumentDatabase.DocumentsStorage.DocumentPut.Recreate<DocumentPutAction.RecreateCounters>(context, docId);
        }

        var lastProcessedKey = hasMore ? docIds[^1] : Completed;
        _database.DocumentsStorage.SetLastFixedCounterKey(context, lastProcessedKey);

        return numOfCounterGroupsFixed;
    }

    internal unsafe int FixCountersForDocument(DocumentsOperationContext context, string documentId)
    {
        List<string> allNames = null;
        LazyStringValue collection = default;

        Table writeTable = default;
        int numOfCounterGroupFixed = 0;
        CollectionName collectionName = default;
        List<IDisposable> memoryScopes = new();
        try
        {
            var table = new Table(_database.DocumentsStorage.CountersStorage.CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice key, separator: SpecialChars.RecordSeparator))
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    var tvr = result.Value.Reader;
                    BlittableJsonReaderObject data;

                    using (data = GetCounterValuesData(context, ref tvr))
                    {
                        data = data.Clone(context);
                    }

                    data.TryGet(Values, out BlittableJsonReaderObject counterValues);
                    data.TryGet(CounterNames, out BlittableJsonReaderObject counterNames);
                    data.TryGet(DbIds, out BlittableJsonReaderArray dbIds);

                    bool corruptedDbIds = false;
                    bool corruptedNames = false;

                    for (int i = 0; i < dbIds.Length; i++)
                    {
                        var lsv = dbIds[i] as LazyStringValue;
                        if (IsBase64String(lsv) == false)
                        {
                            corruptedDbIds = true;
                            dbIds.Modifications ??= new DynamicJsonArray();
                            dbIds.Modifications.RemoveAt(i);
                        }
                    }

                    if (counterValues.Count == counterNames.Count)
                    {
                        var counterValuesPropertyNames = counterValues.GetSortedPropertyNames();
                        var counterNamesPropertyNames = counterNames.GetSortedPropertyNames();
                        if (counterValuesPropertyNames.SequenceEqual(counterNamesPropertyNames) == false)
                        {
                            corruptedNames = true;
                        }
                    }

                    if (corruptedDbIds == false && corruptedNames == false)
                        continue;

                    data.Modifications = new DynamicJsonValue(data);

                    if (collection == null)
                    {
                        collection = TableValueToId(context, (int)Counters.CountersTable.Collection, ref tvr);
                        collectionName = _database.DocumentsStorage.ExtractCollectionName(context, collection);

                        writeTable = _database.DocumentsStorage.CountersStorage.GetOrCreateTable(context.Transaction.InnerTransaction, _database.DocumentsStorage.CountersStorage.CountersSchema, collectionName, CollectionTableType.CounterGroups);
                    }

                    if (corruptedNames)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        var originalNames = new DynamicJsonValue();

                        for (int i = 0; i < counterValues.Count; i++)
                        {
                            counterValues.GetPropertyByIndex(i, ref prop);

                            var lowerCasedCounterName = prop.Name;
                            if (counterNames.TryGet(lowerCasedCounterName, out string counterNameToUse) == false)
                            {
                                // CounterGroup document is corrupted - missing counter name
                                allNames ??= _database.DocumentsStorage.CountersStorage.GetCountersForDocument(context, documentId).ToList();
                                var location = allNames.BinarySearch(lowerCasedCounterName, StringComparer.OrdinalIgnoreCase);

                                // if we don't have the counter name in its original casing - we'll use the lowered-case name instead
                                counterNameToUse = location < 0
                                    ? lowerCasedCounterName
                                    : allNames[location];
                            }

                            originalNames[lowerCasedCounterName] = counterNameToUse;
                        }

                        data.Modifications[CounterNames] = originalNames;
                    }

                    if (corruptedDbIds)
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;

                        for (int i = 0; i < counterValues.Count; i++)
                        {
                            counterValues.GetPropertyByIndex(i, ref prop);
                            if (prop.Value is not BlittableJsonReaderObject.RawBlob blob) 
                                continue;
                            
                            // remove badDbId entries from blob
                            var existingCountOfValues = blob.Length / SizeOfCounterValues;
                            if (existingCountOfValues <= dbIds.Modifications.Removals[0])  
                                continue; // counter is not affected by these dbId removals

                            var numOfDbIdRemovals = dbIds.Modifications.Removals.Count; 
                            var newCountOfValues = existingCountOfValues - numOfDbIdRemovals;

                            memoryScopes.Add(context.Allocator.Allocate(newCountOfValues * SizeOfCounterValues, out var newVal));

                            var currentIndex = 0;
                            for (var index = 0; index < existingCountOfValues; index++)
                            {
                                if (dbIds.Modifications.Removals.Contains(index))
                                    continue; // this dbId is corrupted, skip it

                                var existingValue = &((CounterValues*)blob.Address)[index];
                                var newEntry = (CounterValues*)newVal.Ptr + currentIndex;

                                newEntry->Value = existingValue->Value;
                                newEntry->Etag = existingValue->Etag;

                                currentIndex++;
                            }

                            blob.Address = newVal.Ptr;
                            blob.Length = newVal.Length;

                            counterValues.Modifications ??= new DynamicJsonValue(counterValues);
                            counterValues.Modifications[prop.Name] = blob;
                        }
                    }

                    using (var old = data)
                    {
                        data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    }

                    // we're using the same change vector and etag here, in order to avoid replicating
                    // the counter group to other nodes (each node should fix its counters locally)
                    using var changeVector = TableValueToString(context, (int)Counters.CountersTable.ChangeVector, ref tvr);
                    var groupEtag = TableValueToEtag((int)Counters.CountersTable.Etag, ref tvr);

                    using (var counterGroupKey = TableValueToString(context, (int)Counters.CountersTable.CounterKey, ref tvr))
                    using (context.Allocator.Allocate(counterGroupKey.Size, out var buffer))
                    {
                        counterGroupKey.CopyTo(buffer.Ptr);

                        using (var clonedKey = context.AllocateStringValue(null, buffer.Ptr, buffer.Length))
                        using (Slice.External(context.Allocator, clonedKey, out var countersGroupKey))
                        using (Slice.From(context.Allocator, changeVector, out var cv))
                        using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                        using (writeTable.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(countersGroupKey);
                            tvb.Add(Bits.SwapBytes(groupEtag));
                            tvb.Add(cv);
                            tvb.Add(data.BasePointer, data.Size);
                            tvb.Add(collectionSlice);
                            tvb.Add(context.GetTransactionMarker());

                            writeTable.Set(tvb);
                        }
                    }

                    numOfCounterGroupFixed++;
                }
            }
        }
        finally
        {
            collection?.Dispose();
            foreach (var scope in memoryScopes)
            {
                scope.Dispose();
            }
        }

        return numOfCounterGroupFixed;

    }

    private class StartAfterSliceHolder : IDisposable
    {
        private readonly string _docId;

        private readonly List<IDisposable> _toDispose = [];

        public StartAfterSliceHolder(string docId)
        {
            _docId = docId;
        }

        public void Dispose()
        {
            foreach (var scope in _toDispose)
            {
                scope.Dispose();
            }

            _toDispose.Clear();
        }

        public unsafe Slice GetStartAfterSlice(DocumentsOperationContext context)
        {
            _toDispose.Add(DocumentIdWorker.GetSliceFromId(context, _docId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator));
            _toDispose.Add(context.Allocator.Allocate(documentKeyPrefix.Size + sizeof(long), out var startAfterBuffer));
            _toDispose.Add(Slice.External(context.Allocator, startAfterBuffer.Ptr, startAfterBuffer.Length, out var startAfter));

            documentKeyPrefix.CopyTo(startAfterBuffer.Ptr);
            *(long*)(startAfterBuffer.Ptr + documentKeyPrefix.Size) = long.MaxValue;

            return startAfter;
        }
    }

    private class ExecuteFixCounterGroupsCommand : DocumentMergedTransactionCommand
    {
        private readonly List<string> _docIds;
        private readonly bool _hasMore;
        private readonly DocumentDatabase _database;

        public ExecuteFixCounterGroupsCommand(DocumentDatabase database, List<string> docIdsToFix, bool hasMore)
        {
            _docIds = docIdsToFix;
            _hasMore = hasMore;
            _database = database;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var numOfCounterGroupFixed = _database.CountersRepairTask.FixCountersForDocuments(context, _docIds, _hasMore);
            return numOfCounterGroupFixed;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
