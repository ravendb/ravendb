using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Data.Compression;
using Constants = Voron.Global.Constants;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract unsafe class ReduceMapResultsBase<T> : IIndexingWork where T : IndexDefinitionBaseServerSide
    {
        private static readonly TimeSpan MinReduceDurationToCalculateProcessMemoryUsage = TimeSpan.FromSeconds(3);
        internal static readonly Slice PageNumberSlice;
        internal static readonly string PageNumberToReduceResultTableName = "PageNumberToReduceResult";
        private readonly Logger _logger;
        private readonly AggregationBatch _aggregationBatch = new AggregationBatch();
        private readonly Index _index;
        protected readonly T _indexDefinition;
        private readonly IndexStorage _indexStorage;
        private readonly MetricCounters _metrics;
        private readonly MapReduceIndexingContext _mapReduceContext;

        internal static readonly TableSchema ReduceResultsSchema;

        private IndexingStatsScope _treeReductionStatsInstance;
        private IndexingStatsScope _nestedValuesReductionStatsInstance;
        private readonly TreeReductionStats _treeReductionStats = new TreeReductionStats();
        private readonly NestedValuesReductionStats _nestedValuesReductionStats = new NestedValuesReductionStats();

        internal const int NumberOfResultsPosition = 2;
        internal const int StartOutputResultsPosition = 3;


        protected ReduceMapResultsBase(Index index, T indexDefinition, IndexStorage indexStorage, MetricCounters metrics, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _indexDefinition = indexDefinition;
            _indexStorage = indexStorage;
            _metrics = metrics;
            _mapReduceContext = mapReduceContext;
            _logger = LoggingSource.Instance.GetLogger<ReduceMapResultsBase<T>>(indexStorage.DocumentDatabase.Name);
        }

        static ReduceMapResultsBase()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "PageNumber", ByteStringType.Immutable, out PageNumberSlice);
            }
            ReduceResultsSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                    Count = 1,
                    Name = PageNumberSlice
                });
        }

        public string Name { get; } = "Reduce";

        public bool Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation,
                            IndexingStatsScope stats, CancellationToken token)
        {
            if (_mapReduceContext.StoreByReduceKeyHash.Count == 0)
            {
                WriteLastEtags(indexContext); // we need to write etags here, because if we filtered everything during map then we will loose last indexed etag information and this will cause an endless indexing loop
                return false;
            }

            ReduceResultsSchema.Create(indexContext.Transaction.InnerTransaction, PageNumberToReduceResultTableName, 32);
            var table = indexContext.Transaction.InnerTransaction.OpenTable(ReduceResultsSchema, PageNumberToReduceResultTableName);

            var lowLevelTransaction = indexContext.Transaction.InnerTransaction.LowLevelTransaction;

            var writer = writeOperation.Value;

            var treeScopeStats = stats.For(IndexingOperation.Reduce.TreeScope, start: false);
            var nestedValuesScopeStats = stats.For(IndexingOperation.Reduce.NestedValuesScope, start: false);

            foreach (var store in _mapReduceContext.StoreByReduceKeyHash)
            {
                token.ThrowIfCancellationRequested();
                
                using (var reduceKeyHash = indexContext.GetLazyString(store.Key.ToString(CultureInfo.InvariantCulture)))
                using (store.Value)
                using (_aggregationBatch)
                {
                    var modifiedStore = store.Value;

                    switch (modifiedStore.Type)
                    {
                        case MapResultsStorageType.Tree:
                            using (treeScopeStats.Start())
                            {
                                HandleTreeReduction(indexContext, treeScopeStats, modifiedStore, lowLevelTransaction, writer, reduceKeyHash, table, token);
                            }
                            break;
                        case MapResultsStorageType.Nested:
                            using (nestedValuesScopeStats.Start())
                            {
                                HandleNestedValuesReduction(indexContext, nestedValuesScopeStats, modifiedStore, writer, reduceKeyHash, token);
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException(modifiedStore.Type.ToString());
                    }
                }

                if (_mapReduceContext.FreedPages.Count > 0)
                {
                    long tmp = 0;
                    using (treeScopeStats.Start())
                    using (Slice.External(indexContext.Allocator, (byte*)&tmp, sizeof(long), out Slice pageNumberSlice))
                    {
                        foreach (var freedPage in _mapReduceContext.FreedPages)
                        {
                            tmp = Bits.SwapBytes(freedPage);
                            table.DeleteByKey(pageNumberSlice);
                        }
                    }
                }
            }

            if (stats.Duration >= MinReduceDurationToCalculateProcessMemoryUsage)
            {
                var workingSet = MemoryInformation.GetWorkingSetInBytes();
                var privateMemory = AbstractLowMemoryMonitor.GetManagedMemoryInBytes() + AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes();
                stats.RecordReduceMemoryStats(workingSet, privateMemory);
            }

            WriteLastEtags(indexContext);
            _mapReduceContext.StoreNextMapResultId();

            return false;
        }

        public bool CanContinueBatch(QueryOperationContext queryContext, TransactionOperationContext indexingContext, 
            IndexingStatsScope stats, IndexWriteOperation indexWriteOperation, long currentEtag, long maxEtag, long count)
        {
            throw new NotSupportedException();
        }

        private void WriteLastEtags(TransactionOperationContext indexContext)
        {
            foreach (var lastEtag in _mapReduceContext.ProcessedDocEtags)
            {
                _indexStorage.WriteLastIndexedEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }

            foreach (var lastEtag in _mapReduceContext.ProcessedTombstoneEtags)
            {
                _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }
        }

        private void HandleNestedValuesReduction(TransactionOperationContext indexContext, IndexingStatsScope stats,
                    MapReduceResultsStore modifiedStore,
                    IndexWriteOperation writer, LazyStringValue reduceKeyHash, CancellationToken token)
        {
            EnsureValidNestedValuesReductionStats(stats);

            var numberOfEntriesToReduce = 0;

            try
            {
                var section = modifiedStore.GetNestedResultsSection();

                if (section.IsModified == false)
                    return;

                using (_nestedValuesReductionStats.NestedValuesRead.Start())
                {
                    numberOfEntriesToReduce += section.GetResults(indexContext, _aggregationBatch.Items);
                }

                stats.RecordReduceAttempts(numberOfEntriesToReduce);

                AggregationResult result;
                using (_nestedValuesReductionStats.NestedValuesAggregation.Start())
                {
                    result = AggregateOn(_aggregationBatch.Items, indexContext, _nestedValuesReductionStats.NestedValuesAggregation, token);
                }

                if (section.IsNew == false)
                    writer.DeleteReduceResult(reduceKeyHash, stats);

                foreach (var output in result.GetOutputs())
                {
                    writer.IndexDocument(reduceKeyHash, null, output, stats, indexContext);
                }

                _index.ReducesPerSec?.MarkSingleThreaded(numberOfEntriesToReduce);
                _metrics.MapReduceIndexes.ReducedPerSec.Mark(numberOfEntriesToReduce);

                stats.RecordReduceSuccesses(numberOfEntriesToReduce);

                _index.UpdateThreadAllocations(indexContext, writer, stats, updateReduceStats: true);
                
            }
            catch (Exception e) when (e.IsIndexError())
            {
                _index.ErrorIndexIfCriticalException(e);

                HandleReductionError(e, reduceKeyHash, writer, stats, updateStats: true, page: null, numberOfNestedValues: numberOfEntriesToReduce);
            }
        }

        private void HandleTreeReduction(TransactionOperationContext indexContext, IndexingStatsScope stats,
             MapReduceResultsStore modifiedStore, LowLevelTransaction lowLevelTransaction,
            IndexWriteOperation writer, LazyStringValue reduceKeyHash, Table table, CancellationToken token)
        {
            if (modifiedStore.ModifiedPages.Count == 0)
                return;

            EnsureValidTreeReductionStats(stats);

            var tree = modifiedStore.Tree;

            var branchesToAggregate = new HashSet<long>();

            var parentPagesToAggregate = new HashSet<long>();

            var page = new TreePage(null, Constants.Storage.PageSize);

            HashSet<long> compressedEmptyLeafs = null;

            Dictionary<long, Exception> failedAggregatedLeafs = null;

            foreach (var modifiedPage in modifiedStore.ModifiedPages)
            {
                token.ThrowIfCancellationRequested();

                page.Base = lowLevelTransaction.GetPage(modifiedPage).Pointer;

                stats.RecordReduceTreePageModified(page.IsLeaf);

                if (page.IsLeaf == false)
                {
                    Debug.Assert(page.IsBranch);
                    branchesToAggregate.Add(modifiedPage);

                    continue;
                }

                var leafPage = page;

                var compressed = leafPage.IsCompressed;

                if (compressed)
                    stats.RecordCompressedLeafPage();

                using (compressed ? (DecompressedLeafPage)(leafPage = tree.DecompressPage(leafPage, skipCache: true)) : null)
                {
                    if (leafPage.NumberOfEntries == 0)
                    {
                        if (leafPage.PageNumber == tree.State.RootPageNumber)
                        {
                            writer.DeleteReduceResult(reduceKeyHash, stats);

                            var emptyPageNumber = Bits.SwapBytes(leafPage.PageNumber);
                            using (Slice.External(indexContext.Allocator, (byte*)&emptyPageNumber, sizeof(long), out Slice pageNumSlice))
                                table.DeleteByKey(pageNumSlice);

                            continue;
                        }

                        if (compressed)
                        {
                            // it doesn't have any entries after decompression because 
                            // each compressed entry has the delete tombstone

                            if (compressedEmptyLeafs == null)
                                compressedEmptyLeafs = new HashSet<long>();

                            compressedEmptyLeafs.Add(leafPage.PageNumber);
                            continue;
                        }

                        throw new UnexpectedReduceTreePageException(
                            $"Encountered empty page which isn't a root. Page {leafPage} in '{tree.Name}' tree (tree state: {tree.State})");
                    }

                    var parentPage = tree.GetParentPageOf(leafPage);

                    stats.RecordReduceAttempts(leafPage.NumberOfEntries);

                    try
                    {
                        using (var result = AggregateLeafPage(leafPage, lowLevelTransaction, indexContext, token))
                        {
                            if (parentPage == -1)
                            {
                                writer.DeleteReduceResult(reduceKeyHash, stats);

                                foreach (var output in result.GetOutputs())
                                {
                                    writer.IndexDocument(reduceKeyHash, null, output, stats, indexContext);
                                }
                            }
                            else
                            {
                                StoreAggregationResult(leafPage, table, result);
                                parentPagesToAggregate.Add(parentPage);
                            }

                            _index.ReducesPerSec?.MarkSingleThreaded(leafPage.NumberOfEntries);
                            _metrics.MapReduceIndexes.ReducedPerSec.Mark(leafPage.NumberOfEntries);

                            stats.RecordReduceSuccesses(leafPage.NumberOfEntries);
                        }
                    }
                    catch (Exception e) when (e.IsIndexError())
                    {
                        if (failedAggregatedLeafs == null)
                            failedAggregatedLeafs = new Dictionary<long, Exception>();

                        failedAggregatedLeafs.Add(leafPage.PageNumber, e);

                        _index.ErrorIndexIfCriticalException(e);

                        HandleReductionError(e, reduceKeyHash, writer, stats, updateStats: parentPage == -1, page: leafPage);
                    }
                }
            }

            while (parentPagesToAggregate.Count > 0 || branchesToAggregate.Count > 0)
            {
                token.ThrowIfCancellationRequested();

                var branchPages = parentPagesToAggregate;
                parentPagesToAggregate = new HashSet<long>();

                foreach (var pageNumber in branchPages)
                {
                    page.Base = lowLevelTransaction.GetPage(pageNumber).Pointer;

                    try
                    {
                        if (page.IsBranch == false)
                        {
                            throw new UnexpectedReduceTreePageException("Parent page was found that wasn't a branch, error at " + page);
                        }

                        stats.RecordReduceAttempts(page.NumberOfEntries);

                        var parentPage = tree.GetParentPageOf(page);

                        using (var result = AggregateBranchPage(page, table, indexContext, branchesToAggregate, compressedEmptyLeafs, failedAggregatedLeafs, tree, token))
                        {
                            if (parentPage == -1)
                            {
                                writer.DeleteReduceResult(reduceKeyHash, stats);

                                foreach (var output in result.GetOutputs())
                                {
                                    writer.IndexDocument(reduceKeyHash, null, output, stats, indexContext);
                                }
                            }
                            else
                            {
                                parentPagesToAggregate.Add(parentPage);

                                StoreAggregationResult(page, table, result);
                            }

                            _index.ReducesPerSec?.MarkSingleThreaded(page.NumberOfEntries);
                            _metrics.MapReduceIndexes.ReducedPerSec.Mark(page.NumberOfEntries);

                            stats.RecordReduceSuccesses(page.NumberOfEntries);
                        }
                    }
                    catch (Exception e) when (e.IsIndexError())
                    {
                        _index.ErrorIndexIfCriticalException(e);

                        HandleReductionError(e, reduceKeyHash, writer, stats, updateStats: true, page: page);
                    }
                    finally
                    {
                        branchesToAggregate.Remove(pageNumber);
                    }
                }

                if (parentPagesToAggregate.Count == 0 && branchesToAggregate.Count > 0)
                {
                    // we still have unaggregated branches which were modified but their children were not modified (branch page splitting) so we missed them
                    parentPagesToAggregate.Add(branchesToAggregate.First());
                }

                _index.UpdateThreadAllocations(indexContext, writer, stats, updateReduceStats: true);
            }

            if (compressedEmptyLeafs != null && compressedEmptyLeafs.Count > 0)
            {
                // we had some compressed pages that are empty after decompression
                // let's remove them and reduce the tree once again

                modifiedStore.ModifiedPages.Clear();

                foreach (var pageNumber in compressedEmptyLeafs)
                {
                    page.Base = lowLevelTransaction.GetPage(pageNumber).Pointer;

                    using (var emptyPage = tree.DecompressPage(page, skipCache: true))
                    {
                        if (emptyPage.NumberOfEntries > 0) // could be changed meanwhile
                            continue;

                        modifiedStore.Tree.RemoveEmptyDecompressedPage(emptyPage);
                    }
                }

                HandleTreeReduction(indexContext, stats, modifiedStore, lowLevelTransaction, writer, reduceKeyHash, table, token);
            }
        }

        private AggregationResult AggregateLeafPage(TreePage page,
            LowLevelTransaction lowLevelTransaction,
            TransactionOperationContext indexContext,
            CancellationToken token)
        {
            using (_treeReductionStats.LeafAggregation.Start())
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                    var reduceEntry = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, indexContext);

                    _aggregationBatch.Items.Add(reduceEntry);
                }

                return AggregateBatchResults(_aggregationBatch.Items, indexContext, _treeReductionStats.LeafAggregation, token);
            }
        }

        private AggregationResult AggregateBranchPage(TreePage page,
            Table table,
            TransactionOperationContext indexContext,
            HashSet<long> remainingBranchesToAggregate,
            HashSet<long> compressedEmptyLeafs,
            Dictionary<long, Exception> failedAggregatedLeafs,
            Tree tree,
            CancellationToken token)
        {
            using (_treeReductionStats.BranchAggregation.Start())
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var pageNumber = page.GetNode(i)->PageNumber;
                    var childPageNumber = Bits.SwapBytes(pageNumber);
                    using (Slice.External(indexContext.Allocator, (byte*)&childPageNumber, sizeof(long), out Slice childPageNumberSlice))
                    {
                        if (table.ReadByKey(childPageNumberSlice, out TableValueReader tvr) == false)
                        {
                            if (TryAggregateChildPageOrThrow(pageNumber, table, indexContext, remainingBranchesToAggregate, compressedEmptyLeafs, failedAggregatedLeafs, tree, token))
                            {
                                table.ReadByKey(childPageNumberSlice, out tvr);
                            }
                            else
                            {
                                continue;
                            }
                        }

                        var numberOfResults = *(int*)tvr.Read(NumberOfResultsPosition, out int size);

                        for (int j = 0; j < numberOfResults; j++)
                        {
                            _aggregationBatch.Items.Add(new BlittableJsonReaderObject(tvr.Read(StartOutputResultsPosition + j, out size), size, indexContext));
                        }
                    }
                }

                return AggregateBatchResults(_aggregationBatch.Items, indexContext, _treeReductionStats.BranchAggregation, token);
            }
        }

        private AggregationResult AggregateBatchResults(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, IndexingStatsScope stats, CancellationToken token)
        {
            AggregationResult result;

            try
            {
                result = AggregateOn(aggregationBatch, indexContext, stats, token);
            }
            finally
            {
                aggregationBatch.Clear();
            }

            return result;
        }

        private void StoreAggregationResult(TreePage page, Table table, AggregationResult result)
        {
            using (_treeReductionStats.StoringReduceResult.Start())
            {
                var pageNumber = Bits.SwapBytes(page.PageNumber);
                var numberOfOutputs = result.Count;

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(pageNumber);
                    tvb.Add(page.NumberOfEntries);
                    tvb.Add(numberOfOutputs);

                    foreach (var output in result.GetOutputsToStore())
                    {
                        tvb.Add(output.BasePointer, output.Size);
                    }

                    table.Set(tvb);
                }
            }
        }

        private bool TryAggregateChildPageOrThrow(long pageNumber, Table table, TransactionOperationContext indexContext,
            HashSet<long> remainingBranchesToAggregate,
            HashSet<long> compressedEmptyLeafs,
            Dictionary<long, Exception> failedAggregatedLeafs,
            Tree tree,
            CancellationToken token)
        {
            if (remainingBranchesToAggregate.Contains(pageNumber))
            {
                // RavenDB-5363: we have a modified branch page but its children were not modified (branch page splitting) so we didn't
                // aggregated it yet, let's do it now

                try
                {
                    var page = indexContext.Transaction.InnerTransaction.LowLevelTransaction.GetPage(pageNumber);
                    var unaggregatedBranch = new TreePage(page.Pointer, Constants.Storage.PageSize);

                    using (var result = AggregateBranchPage(unaggregatedBranch, table, indexContext, remainingBranchesToAggregate, compressedEmptyLeafs,
                        failedAggregatedLeafs, tree, token))
                    {
                        StoreAggregationResult(unaggregatedBranch, table, result);
                    }
                }
                finally
                {
                    remainingBranchesToAggregate.Remove(pageNumber);
                }

                return true;
            }

            if (compressedEmptyLeafs != null && compressedEmptyLeafs.Contains(pageNumber))
            {
                // it's empty after decompression, we can safely skip it here

                return false;
            }

            var relatedPage = indexContext.Transaction.InnerTransaction.LowLevelTransaction.GetPage(pageNumber);
            var relatedTreePage = new TreePage(relatedPage.Pointer, Constants.Storage.PageSize);

            string decompressedDebug = null;

            if (relatedTreePage.IsCompressed)
            {
                // let's try to decompress it and check if it's empty
                // we decompress it for validation purposes only although it's very rare case

                using (var decompressed = tree.DecompressPage(relatedTreePage, skipCache: true))
                {
                    if (decompressed.NumberOfEntries == 0)
                    {
                        // it's empty so there is no related aggregation result, we can safely skip it

                        return false;
                    }

                    decompressedDebug = decompressed.ToString();
                }
            }

            var message = $"Couldn't find a pre-computed aggregation result for the existing page: {relatedTreePage.PageNumber}. ";

            var debugDetails = $"Debug details - page: {relatedTreePage}, ";

            if (decompressedDebug != null)
                debugDetails += $"decompressed: {decompressedDebug}), ";

            debugDetails += $"tree state: {tree.State}. ";

            if (failedAggregatedLeafs != null && failedAggregatedLeafs.TryGetValue(pageNumber, out var exception))
            {
                message += $"The aggregation of this leaf (#{pageNumber}) has failed so the relevant result doesn't exist. " +
                           "Check the inner exception for leaf aggregation error details. ";

                throw new AggregationResultNotFoundException(message + debugDetails, exception);
            }

            message += "Please check if there are other aggregate failures at earlier phase of the reduce stage. They could lead to this error due to missing intermediate results. ";

            throw new AggregationResultNotFoundException(message + debugDetails);
        }

        protected abstract AggregationResult AggregateOn(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, IndexingStatsScope stats, CancellationToken token);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidTreeReductionStats(IndexingStatsScope stats)
        {
            if (_treeReductionStatsInstance == stats)
                return;

            _treeReductionStatsInstance = stats;

            _treeReductionStats.LeafAggregation = stats.For(IndexingOperation.Reduce.LeafAggregation, start: false);
            _treeReductionStats.BranchAggregation = stats.For(IndexingOperation.Reduce.BranchAggregation, start: false);
            _treeReductionStats.StoringReduceResult = stats.For(IndexingOperation.Reduce.StoringReduceResult, start: false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidNestedValuesReductionStats(IndexingStatsScope stats)
        {
            if (_nestedValuesReductionStatsInstance == stats)
                return;

            _nestedValuesReductionStatsInstance = stats;

            _nestedValuesReductionStats.NestedValuesRead = stats.For(IndexingOperation.Reduce.NestedValuesRead, start: false);
            _nestedValuesReductionStats.NestedValuesAggregation = stats.For(IndexingOperation.Reduce.NestedValuesAggregation, start: false);
        }

        private void HandleReductionError(Exception error, LazyStringValue reduceKeyHash, IndexWriteOperation writer, IndexingStatsScope stats, bool updateStats, TreePage page,
            int numberOfNestedValues = -1)
        {
            var builder = new StringBuilder("Failed to execute reduce function on ");

            if (page != null)
                builder.Append($"page {page} ");
            else
                builder.Append("nested values ");

            builder.Append($"of '{_indexDefinition.Name}' index. The relevant reduce result is going to be removed from the index ");
            builder.Append($"as it would be incorrect due to encountered errors (reduce key hash: {reduceKeyHash}");

            var sampleItem = _aggregationBatch?.Items?.FirstOrDefault();

            if (sampleItem != null)
                builder.Append($", sample item to reduce: {sampleItem}");

            builder.Append(")");

            var message = builder.ToString();
            
            if (_logger.IsInfoEnabled)
                _logger.Info(message, error);

            try
            {
                writer.DeleteReduceResult(reduceKeyHash, stats);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to delete an index result from '${_indexDefinition.Name}' index on reduce error (reduce key hash: ${reduceKeyHash})", e);
            }
            
            if (updateStats)
            {
                var numberOfEntries = page?.NumberOfEntries ?? numberOfNestedValues;

                Debug.Assert(numberOfEntries != -1);

                // we'll only want to record exceptions on some of these, to give the 
                // user information about what is going on, otherwise we'll have to wait a lot if we 
                // are processing a big batch, and this can be a perf killer. See: RavenDB-11038
                
                stats.RecordReduceErrors(numberOfEntries);

                if (stats.NumberOfKeptReduceErrors < IndexStorage.MaxNumberOfKeptErrors)
                    stats.AddReduceError(message + $" Exception: {error}");

                var failureInfo = new IndexFailureInformation
                {
                    Name = _index.Name,
                    MapErrors = stats.MapErrors,
                    MapAttempts = stats.MapAttempts,
                    ReduceErrors = stats.ReduceErrors,
                    ReduceAttempts = stats.ReduceAttempts
                };
                
                if (failureInfo.IsInvalidIndex(true))
                {
                    throw new ExcessiveNumberOfReduceErrorsException("Excessive number of errors during the reduce phase for the current batch. Failure info: " +
                                                                     failureInfo.GetErrorMessage());
                }
            }
        }

        private class AggregationBatch : IDisposable
        {
            public readonly List<BlittableJsonReaderObject> Items = new List<BlittableJsonReaderObject>();

            public void Dispose()
            {
                foreach (var item in Items)
                {
                    item.Dispose();
                }

                Items.Clear();
            }
        }

        private class TreeReductionStats
        {
            public IndexingStatsScope LeafAggregation;
            public IndexingStatsScope BranchAggregation;
            public IndexingStatsScope StoringReduceResult;
        }

        private class NestedValuesReductionStats
        {
            public IndexingStatsScope NestedValuesRead;
            public IndexingStatsScope NestedValuesAggregation;
        }
    }
}
