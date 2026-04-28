using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Comparers;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedMapReduceStreamingEnumerator : MergedEnumerator<BlittableJsonReaderObject>
{
    private readonly IndexQueryServerSide _query;
    private readonly ShardedDatabaseContext _databaseContext;
    private readonly StreamQueryStatistics _queryStats;
    private readonly ShardedQueryFilter _queryFilter;
    private readonly bool _isProjectionFromMapReduceIndex;
    private readonly TransactionOperationContext _context;
    private readonly CancellationToken _token;

    private IShardedBatchReducer _reducer;
    private ShardedMapReduceResultRetriever _retriever;

    private readonly Queue<BlittableJsonReaderObject> _resultsBuffer = new();
    private readonly List<BlittableJsonReaderObject> _currentBatch = new();
    private readonly List<IDisposable> _enumeratorsToDispose = new();

    public ShardedMapReduceStreamingEnumerator(
        IndexQueryServerSide query,
        ShardedDatabaseContext databaseContext,
        List<OrderByField> reduceKeys,
        bool isProjectionFromMapReduceIndex,
        StreamQueryStatistics queryStats,
        TransactionOperationContext context,
        CancellationToken token)
        : base(CreateComparer(reduceKeys, databaseContext, queryStats))
    {
        _query = query;
        _databaseContext = databaseContext;
        _isProjectionFromMapReduceIndex = isProjectionFromMapReduceIndex;
        _queryStats = queryStats;
        _context = context;
        _token = token;

        if (query.Metadata.Query.Filter != null)
            _queryFilter = new ShardedQueryFilter(query, new ShardedQueryResult(), queryTimings: null, _databaseContext.Indexes.ScriptRunnerCache, _context);
    }

    private static DocumentsComparer CreateComparer(List<OrderByField> reduceKeys, ShardedDatabaseContext databaseContext, StreamQueryStatistics queryStats)
    {
        DocumentsComparer.RetrieveConfigurationForDocumentsComparer(databaseContext, queryStats.IndexName, out var nullIsSmallest, out var acceptMissing);
        return new DocumentsComparer(reduceKeys.ToArray(), extractFromData: true, hasOrderByRandom: false, nullIsSmallest, acceptMissing);
    }

    public override bool MoveNext()
    {
        if (_resultsBuffer.Count > 0)
        {
            CurrentItem = _resultsBuffer.Dequeue();
            return true;
        }

        while (GetNextResult(out var item))
        {
            if (_isProjectionFromMapReduceIndex == false)
            {
                // simple map-reduce (no projection)
                CurrentItem = item;
                return true;
            }

            try
            {
                _retriever ??= InitializeRetriever();

                foreach (BlittableJsonReaderObject result in ShardedQueryProcessor.GetProjectionResults(_retriever, item, _context))
                {
                    _resultsBuffer.Enqueue(result);
                }
            }
            finally
            {
                // we are done with the map-reduce key/result 'item', so we must dispose it.
                // we do not return it to the caller, we return the projected results instead.
                item.Dispose();
            }

            if (_resultsBuffer.Count > 0)
            {
                CurrentItem = _resultsBuffer.Dequeue();
                return true;
            }

            // if buffer is empty (projection yielded no results for this key),
            // loop again to fetch the next key.
        }

        CurrentItem = null;
        return false;
    }

    private bool GetNextResult(out BlittableJsonReaderObject item)
    {
        // loop until we find a valid item or run out of data
        while (WorkEnumerators.Count > 0)
        {
            var minEnumerator = WorkEnumerators[0];
            for (var index = 1; index < WorkEnumerators.Count; index++)
            {
                if (Comparer.Compare(WorkEnumerators[index].Current, minEnumerator.Current) < 0)
                {
                    minEnumerator = WorkEnumerators[index];
                }
            }

            var minKeyItem = minEnumerator.Current;

            _currentBatch.Clear();

            // iterate backwards to easily remove exhausted enumerators
            for (int i = WorkEnumerators.Count - 1; i >= 0; i--)
            {
                var enumerator = WorkEnumerators[i];

                if (Comparer.Compare(enumerator.Current, minKeyItem) == 0)
                {
                    _currentBatch.Add(enumerator.Current);

                    if (enumerator.MoveNext() == false)
                    {
                        _enumeratorsToDispose.Add(WorkEnumerators[i]);
                        WorkEnumerators.RemoveAt(i);
                    }
                }
            }

            // re-reduce on this specific batch
            _reducer ??= CreateBatchReducer();
            item = _reducer.ReduceBatch(_currentBatch);

            if (_queryFilter != null)
            {
                var filterResult = _queryFilter.Apply(item);

                if (filterResult == FilterResult.Skipped)
                {
                    item.Dispose();
                    continue;
                }

                if (filterResult == FilterResult.LimitReached)
                {
                    item.Dispose();
                    item = null;
                    return false;
                }
            }

            return true;
        }

        item = null;
        return false;
    }

    private ShardedMapReduceResultRetriever InitializeRetriever()
    {
        var index = _databaseContext.Indexes.GetIndex(_queryStats.IndexName);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(_queryStats.IndexName);

        var fieldsToFetch = new FieldsToFetch(_query, index.Definition, index.Type);
        return new ShardedMapReduceResultRetriever(
            _databaseContext.Indexes.ScriptRunnerCache,
            _query,
            null,
            fieldsToFetch,
            null,
            _context,
            null,
            null,
            null,
            _databaseContext.IdentityPartsSeparator);
    }

    public override void Dispose()
    {
        using (_reducer)
        {
            foreach (var workEnumerator in _enumeratorsToDispose)
            {
                workEnumerator.Dispose();
            }

            _enumeratorsToDispose.Clear();

            base.Dispose();
        }
    }

    private IShardedBatchReducer CreateBatchReducer()
    {
        var index = _databaseContext.Indexes.GetIndex(_queryStats.IndexName);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(_queryStats.IndexName);

        if (index.Type.IsStaticMapReduce())
        {
            return new ShardedStaticBatchReducer(_databaseContext, index, _context);
        }

        if (index.Type.IsAutoMapReduce())
        {
            return new ShardedAutoBatchReducer(index, ShardedMapReduceQueryResultsMerger.Aggregator, _context, _token);
        }

        throw new InvalidOperationException($"Index '{_queryStats.IndexName}' is not a map-reduce index");
    }

    private interface IShardedBatchReducer : IDisposable
    {
        BlittableJsonReaderObject ReduceBatch(List<BlittableJsonReaderObject> batch);
    }

    private class ShardedStaticBatchReducer : IShardedBatchReducer
    {
        private readonly IndexInformationHolder _index;
        private readonly TransactionOperationContext _context;

        private readonly IndexingFunc _reducingFunc;
        private readonly ReduceMapResultsOfStaticIndex.DynamicIterationOfAggregationBatchWrapper _wrapper;
        private readonly UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        private readonly List<object> _singleResultContainer = new(1);
        private IPropertyAccessor _propertyAccessor;

        public ShardedStaticBatchReducer(ShardedDatabaseContext databaseContext, IndexInformationHolder index, TransactionOperationContext context)
        {
            _index = index;
            _context = context;

            if (index is not StaticIndexInformationHolder staticIndex)
                throw new InvalidOperationException($"Index '{index.Name}' is not a static map-reduce index");

            _reducingFunc = staticIndex.Compiled.Reduce;

            // Initialize the wrapper once. We will just feed it new data in the loop.
            _wrapper = new ReduceMapResultsOfStaticIndex.DynamicIterationOfAggregationBatchWrapper();

            _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling(databaseContext.Loggers.GetLogger<ShardedStaticBatchReducer>(), $"Sharded//Indexes//{index.Name}");
            CurrentIndexingScope.Current = new OrchestratorIndexingScope(_context, _unmanagedBuffersPool);
        }

        public BlittableJsonReaderObject ReduceBatch(List<BlittableJsonReaderObject> batch)
        {
            _wrapper.InitializeForEnumeration(batch);

            _singleResultContainer.Clear();

            foreach (var output in _reducingFunc(_wrapper))
            {
                _propertyAccessor ??= PropertyAccessor.Create(output.GetType(), output);
                _singleResultContainer.Add(output);
            }

            if (_singleResultContainer.Count == 0)
                return null;

            var aggregatedObjects = new ShardedAggregatedAnonymousObjects(
                _singleResultContainer,
                _propertyAccessor,
                _context,
                skipImplicitNullInOutput: _index.Configuration.IndexMissingFieldsAsNull == false);

            foreach (var result in aggregatedObjects.GetOutputsToStore())
            {
                return result;
            }

            return null;
        }

        public void Dispose()
        {
            using (_unmanagedBuffersPool)
            using (CurrentIndexingScope.Current)
            {
            }
        }
    }

    private class ShardedAutoBatchReducer : IShardedBatchReducer
    {
        private readonly AutoMapReduceIndexDefinition _definition;
        private readonly ShardedAutoMapReduceIndexResultsAggregator _aggregator;
        private readonly TransactionOperationContext _context;
        private readonly CancellationToken _token;

        private BlittableJsonReaderObject _reusableResultBuffer = null;

        public ShardedAutoBatchReducer(IndexInformationHolder index, ShardedAutoMapReduceIndexResultsAggregator aggregator, TransactionOperationContext context, CancellationToken token)
        {
            if (index.Type.IsAutoMapReduce() == false)
                throw new InvalidOperationException($"Index '{index.Name}' is not an auto map-reduce index");

            _definition = (AutoMapReduceIndexDefinition)index.Definition;
            _aggregator = aggregator;
            _context = context;
            _token = token;
        }

        public BlittableJsonReaderObject ReduceBatch(List<BlittableJsonReaderObject> batch)
        {
            _reusableResultBuffer = null;
            var aggregateOn = _aggregator.AggregateOn(batch, _definition, _context, null, ref _reusableResultBuffer, _token);
            return aggregateOn.GetOutputsToStore().First();
        }

        public void Dispose()
        {
        }
    }
}
