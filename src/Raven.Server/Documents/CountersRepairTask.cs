using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents;

public class CountersRepairTask
{
    private readonly DocumentDatabase _database;

    private readonly Logger _logger;

    public static string Completed = string.Empty;

    public CountersRepairTask(DocumentDatabase database)
    {
        _database = database;
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
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var table = new Table(CountersStorage.CountersSchema, context.Transaction.InnerTransaction);

                    while (true)
                    {
                        bool hasMore = false;

                        foreach (var (_, tvh) in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, startAfter: startAfterSliceHolder?.GetStartAfterSlice(context) ?? Slices.Empty, skip: 0))
                        {
                            hasMore = true;

                            using (var docId = CountersStorage.ExtractDocId(context, ref tvh.Reader))
                            {
                                if (docId != lastDocId)
                                {
                                    lastDocId = docId;

                                    using (var old = startAfterSliceHolder)
                                    {
                                        startAfterSliceHolder = new StartAfterSliceHolder(lastDocId);
                                    }
                                }

                                using (var data = CountersStorage.GetCounterValuesData(context, ref tvh.Reader))
                                {
                                    data.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counterValues);
                                    data.TryGet(CountersStorage.CounterNames, out BlittableJsonReaderObject counterNames);

                                    if (counterValues.Count == counterNames.Count)
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

    private void MarkAsCompleted()
    {
        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeCtx))
        using (var tx = writeCtx.OpenWriteTransaction())
        {
            _database.DocumentsStorage.SetLastFixedCounterKey(writeCtx, Completed);
            tx.Commit();
        }
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

    public class ExecuteFixCounterGroupsCommand : TransactionOperationsMerger.MergedTransactionCommand
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
            var numOfCounterGroupFixed = _database.DocumentsStorage.CountersStorage.FixCountersForDocuments(context, _docIds, _hasMore);
            return numOfCounterGroupFixed;
        }

        public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
        {
            throw new NotImplementedException();
        }
    }
}
