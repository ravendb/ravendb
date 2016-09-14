using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract unsafe class ReduceMapResultsBase<T> : IIndexingWork where T : IndexDefinitionBase
    {
        public static readonly Slice PageNumberSlice = Slice.From(StorageEnvironment.LabelsContext, "PageNumber", ByteStringType.Immutable);
        private Logger _logger;
        private readonly List<BlittableJsonReaderObject> _aggregationBatch = new List<BlittableJsonReaderObject>();
        protected readonly T _indexDefinition;
        private readonly IndexStorage _indexStorage;
        private readonly MetricsCountersManager _metrics;
        private readonly MapReduceIndexingContext _mapReduceContext;

        private readonly TableSchema _reduceResultsSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                NameAsSlice = PageNumberSlice
            });

        protected ReduceMapResultsBase(T indexDefinition, IndexStorage indexStorage, MetricsCountersManager metrics, MapReduceIndexingContext mapReduceContext)
        {
            _indexDefinition = indexDefinition;
            _indexStorage = indexStorage;
            _metrics = metrics;
            _mapReduceContext = mapReduceContext;
            _logger = LoggingSource.Instance.GetLogger<ReduceMapResultsBase<T>>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Reduce";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation,
                            IndexingStatsScope stats, CancellationToken token)
        {
            if (_mapReduceContext.StoreByReduceKeyHash.Count == 0)
                return false;

            _aggregationBatch.Clear();

            _reduceResultsSchema.Create(indexContext.Transaction.InnerTransaction, "PageNumberToReduceResult");
            var table = indexContext.Transaction.InnerTransaction.OpenTable(_reduceResultsSchema, "PageNumberToReduceResult");

            var lowLevelTransaction = indexContext.Transaction.InnerTransaction.LowLevelTransaction;
            

            var writer = writeOperation.Value;

            foreach (var store in _mapReduceContext.StoreByReduceKeyHash)
            {
                using (var reduceKeyHash = indexContext.GetLazyString(store.Key.ToString(CultureInfo.InvariantCulture)))
                using (store.Value)
                {
                    var modifiedStore = store.Value;

                    switch (modifiedStore.Type)
                    {
                        case MapResultsStorageType.Tree:
                            HandleTreeReduction(indexContext, stats, token, modifiedStore, lowLevelTransaction, writer, reduceKeyHash, table);
                            break;
                        case MapResultsStorageType.Nested:
                            HandleNestedValuesReduction(indexContext, stats, token, modifiedStore, writer, reduceKeyHash);
                            break;

                        default:
                            throw new ArgumentOutOfRangeException(modifiedStore.Type.ToString());
                    }
                }
            }

            foreach (var lastEtag in _mapReduceContext.ProcessedDocEtags)
            {
                _indexStorage.WriteLastIndexedEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }

            foreach (var lastEtag in _mapReduceContext.ProcessedTombstoneEtags)
            {
                _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }

            return false;
        }

        private void HandleNestedValuesReduction(TransactionOperationContext indexContext, IndexingStatsScope stats, 
                    CancellationToken token, MapReduceResultsStore modifiedStore, 
                    IndexWriteOperation writer, LazyStringValue reduceKeyHash)
        {
            var numberOfEntriesToReduce = 0;

            try
            {
                modifiedStore.FlushNestedValues();

                var section = modifiedStore.GetNestedResultsSection();

                foreach (var mapResult in section.GetResults())
                {
                    _aggregationBatch.Add(mapResult.Value);
                    numberOfEntriesToReduce++;
                }

                stats.RecordReduceAttempts(numberOfEntriesToReduce);

                var result = AggregateOn(_aggregationBatch, indexContext, token);

                if (section.IsNew == false)
                    writer.DeleteReduceResult(reduceKeyHash, stats);

                foreach (var output in result.Outputs)
                {
                    writer.IndexDocument(reduceKeyHash, new Document
                    {
                        Key = reduceKeyHash,
                        LoweredKey = reduceKeyHash,
                        Data = output
                    }, stats);
                }

                _metrics.MapReduceReducedPerSecond.Mark(numberOfEntriesToReduce);

                stats.RecordReduceSuccesses(numberOfEntriesToReduce);
            }
            catch (Exception e)
            {
                foreach (var item in _aggregationBatch)
                {
                    item.Dispose();
                }

                var message = $"Failed to execute reduce function for reduce key '{reduceKeyHash}' on nested values of '{_indexDefinition.Name}' index.";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                stats.RecordReduceErrors(numberOfEntriesToReduce);
                stats.AddReduceError(message + $" Message: {message}.");
            }
            finally
            {
                _aggregationBatch.Clear();
            }
        }

        private void HandleTreeReduction(TransactionOperationContext indexContext, IndexingStatsScope stats,
            CancellationToken token, MapReduceResultsStore modifiedStore, LowLevelTransaction lowLevelTransaction,
            IndexWriteOperation writer, LazyStringValue reduceKeyHash, Table table)
        {
            var parentPagesToAggregate = new Dictionary<long, Tree>();

            foreach (var modifiedPage in modifiedStore.ModifiedPages)
            {
                token.ThrowIfCancellationRequested();

                if (modifiedStore.FreedPages.Contains(modifiedPage))
                    continue;

                var page = lowLevelTransaction.GetPage(modifiedPage).ToTreePage();
                if (page.IsLeaf == false)
                    continue;

                if (page.NumberOfEntries == 0)
                {
                    if (page.PageNumber != modifiedStore.Tree.State.RootPageNumber)
                    {
                        throw new InvalidOperationException(
                            $"Encountered empty page which isn't a root. Page #{page.PageNumber} in '{modifiedStore.Tree.Name}' tree.");
                    }

                    writer.DeleteReduceResult(reduceKeyHash, stats);

                    var emptyPageNumber = page.PageNumber;
                    table.DeleteByKey(Slice.External(indexContext.Allocator, (byte*) &emptyPageNumber, sizeof(long)));

                    continue;
                }

                var parentPage = modifiedStore.Tree.GetParentPageOf(page);

                stats.RecordReduceAttempts(page.NumberOfEntries);

                try
                {
                    using (var result = AggregateLeafPage(page, lowLevelTransaction, table, indexContext, token))
                    {
                        if (parentPage == -1)
                        {
                            writer.DeleteReduceResult(reduceKeyHash, stats);

                            foreach (var output in result.Outputs)
                            {
                                writer.IndexDocument(reduceKeyHash, new Document
                                {
                                    Key = reduceKeyHash,
                                    LoweredKey = reduceKeyHash,
                                    Data = output
                                }, stats);
                            }

                            _metrics.MapReduceReducedPerSecond.Mark(page.NumberOfEntries);

                            stats.RecordReduceSuccesses(page.NumberOfEntries);
                        }
                        else
                        {
                            parentPagesToAggregate[parentPage] = modifiedStore.Tree;
                        }
                    }
                }
                catch (Exception e)
                {
                    var message =
                        $"Failed to execute reduce function for reduce key '{modifiedStore.Tree.Name}' on a leaf page #{page} of '{_indexDefinition.Name}' index.";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message, e);

                    if (parentPage == -1)
                    {
                        stats.RecordReduceErrors(page.NumberOfEntries);
                        stats.AddReduceError(message + $" Message: {message}.");
                    }
                }
            }

            long tmp = 0;
            var pageNumberSlice = Slice.External(indexContext.Allocator, (byte*) &tmp, sizeof(long));
            foreach (var freedPage in modifiedStore.FreedPages)
            {
                tmp = freedPage;
                table.DeleteByKey(pageNumberSlice);
            }

            while (parentPagesToAggregate.Count > 0)
            {
                token.ThrowIfCancellationRequested();

                var other = parentPagesToAggregate;
                parentPagesToAggregate = new Dictionary<long, Tree>();

                foreach (var kvp in other)
                {
                    var pageNumber = kvp.Key;
                    var tree = kvp.Value;
                    var page = lowLevelTransaction.GetPage(pageNumber).ToTreePage();

                    if (page.IsBranch == false)
                    {
                        throw new InvalidOperationException("Parent page was found that wasn't a branch, error at " +
                                                            page.PageNumber);
                    }

                    var parentPage = tree.GetParentPageOf(page);

                    int aggregatedEntries = 0;

                    try
                    {
                        using (var result = AggregateBranchPage(page, table, indexContext, token, out aggregatedEntries))
                        {
                            if (parentPage == -1)
                            {
                                writer.DeleteReduceResult(reduceKeyHash, stats);

                                foreach (var output in result.Outputs)
                                {
                                    writer.IndexDocument(reduceKeyHash, new Document
                                    {
                                        Key = reduceKeyHash,
                                        LoweredKey = reduceKeyHash,
                                        Data = output
                                    }, stats);
                                }
                                _metrics.MapReduceReducedPerSecond.Mark(aggregatedEntries);

                                stats.RecordReduceSuccesses(aggregatedEntries);
                            }
                            else
                            {
                                parentPagesToAggregate[parentPage] = tree;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        var message =
                            $"Failed to execute reduce function for reduce key '{modifiedStore.Tree.Name}' on a branch page #{page} of '{_indexDefinition.Name}' index.";

                        if (_logger.IsInfoEnabled)
                            _logger.Info(message, e);

                        stats.RecordReduceErrors(aggregatedEntries);
                        stats.AddReduceError(message + $" Message: {message}.");
                    }
                }
            }
        }

        private AggregationResult AggregateLeafPage(TreePage page, LowLevelTransaction lowLevelTransaction, Table table, TransactionOperationContext indexContext, CancellationToken token)
        {
            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                var reduceEntry = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, indexContext);

                _aggregationBatch.Add(reduceEntry);
            }

            return AggregateBatchResults(_aggregationBatch, page.PageNumber, page.NumberOfEntries, table, indexContext, token);
        }

        private AggregationResult AggregateBranchPage(TreePage page, Table table, TransactionOperationContext indexContext, CancellationToken token, out int aggregatedEntries)
        {
            aggregatedEntries = 0;

            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var childPageNumber = IPAddress.HostToNetworkOrder(page.GetNode(i)->PageNumber);
                var tvr = table.ReadByKey(Slice.External(indexContext.Allocator, (byte*)&childPageNumber, sizeof(long)));
                if (tvr == null)
                {
                    throw new InvalidOperationException("Couldn't find pre-computed results for existing page " + childPageNumber);
                }
                
                int size;
                aggregatedEntries += *(int*)tvr.Read(1, out size);

                var numberOfResults = *(int*)tvr.Read(2, out size);

                for (int j = 0; j < numberOfResults; j++)
                {
                    _aggregationBatch.Add(new BlittableJsonReaderObject(tvr.Read(3 + j, out size), size, indexContext));
                }
            }

            return AggregateBatchResults(_aggregationBatch, page.PageNumber, aggregatedEntries, table, indexContext, token);
        }

        private AggregationResult AggregateBatchResults(List<BlittableJsonReaderObject> aggregationBatch, long modifiedPage, int aggregatedEntries,
            Table table, TransactionOperationContext indexContext, CancellationToken token)
        {
            AggregationResult result;

            try
            {
                result = AggregateOn(aggregationBatch, indexContext, token);
            }
            catch (Exception)
            {
                foreach (var item in aggregationBatch)
                {
                    item.Dispose();
                }

                throw;
            }
            finally
            {
                aggregationBatch.Clear();
            }
            
            var pageNumber = IPAddress.HostToNetworkOrder(modifiedPage);
            var numberOfOutputs = result.Outputs.Count;

            var tvb = new TableValueBuilder
            {
                {(byte*) &pageNumber, sizeof(long)},
                {(byte*) &aggregatedEntries, sizeof (int)},
                {(byte*) &numberOfOutputs, sizeof (int)}
            };

            foreach (var output in result.Outputs)
            {
                tvb.Add(output.BasePointer, output.Size);
            }

            table.Set(tvb);

            return result;
        }

        protected abstract AggregationResult AggregateOn(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, CancellationToken token);
    }
}