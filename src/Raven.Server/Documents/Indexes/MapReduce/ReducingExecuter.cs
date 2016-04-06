using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Raven.Database.Util;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class ReducingExecuter
    {
        private readonly TransactionOperationContext _indexContext;
        private readonly LuceneIndexPersistence _indexPersistence;
        private readonly MetricsCountersManager _metrics;
        private readonly Table _table;
        private readonly Dictionary<ulong, ReduceKeyState> _stateByReduceKeyHash;
        readonly List<BlittableJsonReaderObject> _aggregationBatch = new List<BlittableJsonReaderObject>();

        private readonly TableSchema _reduceResultsSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                Name = "PageNumber",
                StartIndex = 0,
                Count = 1
            });

        public ReducingExecuter(Dictionary<ulong, ReduceKeyState> state, TransactionOperationContext indexContext, LuceneIndexPersistence indexPersistence, MetricsCountersManager metrics)
        {
            _stateByReduceKeyHash = state;
            _indexContext = indexContext;
            _indexPersistence = indexPersistence;
            _metrics = metrics;

            _reduceResultsSchema.Create(indexContext.Transaction.InnerTransaction, "PageNumberToReduceResult");
            _table = new Table(_reduceResultsSchema, "PageNumberToReduceResult", indexContext.Transaction.InnerTransaction);
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var lowLevelTransaction = _indexContext.Transaction.InnerTransaction.LowLevelTransaction;
            var parentPagesToAggregate = new Dictionary<long, Tree>();

            using (var writer = _indexPersistence.OpenIndexWriter(_indexContext.Transaction.InnerTransaction))
            {
                foreach (var state in _stateByReduceKeyHash)
                {
                    var reduceKeyHash = _indexContext.GetLazyString(state.Key.ToString(CultureInfo.InvariantCulture)); // TODO arek - ToString()?
                    var modifiedState = state.Value;

                    foreach (var modifiedPage in modifiedState.ModifiedPages)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (modifiedState.FreedPages.Contains(modifiedPage))
                            continue;

                        var page = lowLevelTransaction.GetPage(modifiedPage).ToTreePage();
                        if (page.IsLeaf == false)
                            continue;

                        var parentPage = modifiedState.Tree.GetParentPageOf(page);
                        if (parentPage != -1)
                            parentPagesToAggregate[parentPage] = modifiedState.Tree;

                        using (var result = AggregateLeafPage(page, lowLevelTransaction, modifiedPage))
                        {
                            //TODO arek handle errors
                            if (parentPage == -1)
                            {
                                writer.IndexDocument(new Document
                                {
                                    Key = reduceKeyHash,
                                    Data = result
                                });
                            }
                        }

                        _metrics.MapReduceReducedPerSecond.Mark();
                    }

                    long tmp = 0;
                    Slice pageNumberSlice = new Slice((byte*)&tmp, sizeof(long));
                    foreach (var freedPage in modifiedState.FreedPages)
                    {
                        tmp = freedPage;
                        _table.DeleteByKey(pageNumberSlice);
                    }

                    while (parentPagesToAggregate.Count > 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var other = parentPagesToAggregate;
                        parentPagesToAggregate = new Dictionary<long, Tree>();

                        foreach (var kvp in other)
                        {
                            var pageNumber = kvp.Key;
                            var tree = kvp.Value;
                            var page = lowLevelTransaction.GetPage(pageNumber).ToTreePage();
                            if (page.IsBranch == false)
                            {
                                //TODO: this is an error
                                throw new InvalidOperationException("Parent page was found that wasn't a branch, error at " + page.PageNumber);
                            }

                            var parentPage = tree.GetParentPageOf(page);
                            if (parentPage != -1)
                                parentPagesToAggregate[parentPage] = tree;

                            for (int i = 0; i < page.NumberOfEntries; i++)
                            {
                                var childPageNumber = page.GetNode(i)->PageNumber;
                                var tvr = _table.ReadByKey(new Slice((byte*)&childPageNumber, sizeof(long)));
                                if (tvr == null)
                                {
                                    //TODO: this is an error
                                    throw new InvalidOperationException(
                                        "Couldn't find pre-computed results for existing page " + childPageNumber);
                                }
                                int size;
                                _aggregationBatch.Add(new BlittableJsonReaderObject(tvr.Read(1, out size), size, _indexContext));
                            }

                            using (var result = AggregateBatchResults(pageNumber))
                            {
                                if (parentPage == -1)
                                {
                                    writer.IndexDocument(new Document
                                    {
                                        Key = reduceKeyHash,
                                        Data = result
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        private BlittableJsonReaderObject AggregateLeafPage(TreePage page, LowLevelTransaction lowLevelTransaction, long modifiedPage)
        {
            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                var reduceEntry = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, _indexContext);
                _aggregationBatch.Add(reduceEntry);
            }

            return AggregateBatchResults(modifiedPage);
        }

        private BlittableJsonReaderObject AggregateBatchResults(long modifiedPage)
        {
            var sum = 0;

            var aggregatedResult = new Dictionary<string, object>();

            foreach (var obj in _aggregationBatch)
            {
                foreach (var propertyName in obj.GetPropertyNames())
                {
                    int cur;
                    string stringValue;

                    if ("Count".Equals(propertyName, StringComparison.OrdinalIgnoreCase) && obj.TryGet(propertyName, out cur))
                    {
                        sum += cur;
                        aggregatedResult[propertyName] = sum;
                    }
                    else if (obj.TryGet(propertyName, out stringValue))
                    {
                        aggregatedResult[propertyName] = stringValue;
                    }
                }
            }

            _aggregationBatch.Clear();

            var djv = new DynamicJsonValue();

            foreach (var aggregate in aggregatedResult)
            {
                djv[aggregate.Key] = aggregate.Value;
            }

            var resultObj = _indexContext.ReadObject(djv, "map/reduce");

            _table.Set(new TableValueBuilder
                {
                    {(byte*) &modifiedPage, sizeof (long)}, // page number
                    {resultObj.BasePointer, resultObj.Size}
                });

            return resultObj;
        }
    }
}