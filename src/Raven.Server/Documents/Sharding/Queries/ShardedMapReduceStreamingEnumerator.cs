using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedMapReduceStreamingEnumerator : MergedEnumerator<BlittableJsonReaderObject>
{
    private readonly StreamQueryStatistics _queryStats;
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private readonly JsonOperationContext _context;
    private readonly IDisposable _returnContext;
    private readonly CancellationToken _token;
    private readonly List<BlittableJsonReaderObject> _currentBatch = new();
    private readonly List<IDisposable> _toDispose = new();
    private IShardedBatchReducer _reducer;

    public ShardedMapReduceStreamingEnumerator(List<OrderByField> reduceKeys, ShardedDatabaseRequestHandler requestHandler, StreamQueryStatistics queryStats, Func<(JsonOperationContext, IDisposable)> allocateJsonContext, CancellationToken token)
        : base(new ReduceKeyComparer(reduceKeys))
    {
        _requestHandler = requestHandler;
        _queryStats = queryStats;
        _token = token;
        (_context, _returnContext) = allocateJsonContext();
    }

    public override bool MoveNext()
    {
        if (WorkEnumerators.Count == 0)
            return false;

        var minEnumerator = WorkEnumerators[0];
        for (var index = 1; index < WorkEnumerators.Count; index++)
        {
            if (Comparer.Compare(WorkEnumerators[index].Current, minEnumerator.Current) < 0)
            {
                minEnumerator = WorkEnumerators[index];
            }
        }

        var minKeyItem = minEnumerator.Current;

        _toDispose.ForEach(x => x.Dispose());
        _toDispose.Clear();
        _currentBatch.Clear();
        CurrentItem?.Dispose();

        // we iterate backwards so we can easily remove exhausted enumerators
        for (int i = WorkEnumerators.Count - 1; i >= 0; i--)
        {
            var enumerator = WorkEnumerators[i];

            if (Comparer.Compare(enumerator.Current, minKeyItem) == 0)
            {
                _currentBatch.Add(enumerator.Current);

                if (enumerator.MoveNext() == false)
                {
                    // cannot dispose the WorkEnumerator here until we sent the current batch
                    _toDispose.Add(WorkEnumerators[i]);
                    WorkEnumerators.RemoveAt(i);
                }
            }
        }

        // re-reduce on this specific batch
        _reducer ??= CreateBatchReducer();
        CurrentItem = _reducer.ReduceBatch(_currentBatch);

        return true;
    }

    public override void Dispose()
    {
        base.Dispose();
        _returnContext.Dispose();
    }

    private sealed class ReduceKeyComparer : IComparer<BlittableJsonReaderObject>
    {
        private readonly List<OrderByField> _reduceKeys;

        public ReduceKeyComparer(List<OrderByField> reduceKeys)
        {
            _reduceKeys = reduceKeys;
        }

        public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
        {
            foreach (var reduceKey in _reduceKeys)
            {
                if (x.TryGet(reduceKey.Name, out object valX) == false)
                    ThrowNotFoundReduceKey(reduceKey);

                if (y.TryGet(reduceKey.Name, out object valY) == false)
                    ThrowNotFoundReduceKey(reduceKey);

                if (valX == null && valY == null)
                    continue;

                if (valX == null)
                    return -1;

                if (valY == null)
                    return 1;

                int result = CompareValues(valX, valY, reduceKey.Ascending);
                if (result != 0)
                    return result;
            }

            return 0;
        }

        private int CompareValues(object x, object y, bool ascending)
        {
            int result;

            if (x is IComparable comparableX)
            {
                try
                {
                    result = comparableX.CompareTo(y);
                }
                catch (ArgumentException)
                {
                    // fallback for mixed number types (e.g. comparing int to long)
                    // this is rare in map-reduce keys but safe to handle.
                    result = Convert.ToDouble(x).CompareTo(Convert.ToDouble(y));
                }
            }
            else
            {
                // fallback for non-comparable types (rare for index keys)
                result = string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
            }

            return ascending ? result : -result;
        }

        private static void ThrowNotFoundReduceKey(OrderByField reduceKey)
        {
            throw new InvalidOperationException($"Reduce key '{reduceKey}' not found in the result object.");
        }
    }

    private IShardedBatchReducer CreateBatchReducer()
    {
        var index = _requestHandler.DatabaseContext.Indexes.GetIndex(_queryStats.IndexName);
        if (index == null)
            IndexDoesNotExistException.ThrowFor(_queryStats.IndexName);

        if (index.Type.IsStaticMapReduce())
        {
            return new ShardedStaticBatchReducer(index, _context);
        }

        if (index.Type.IsAutoMapReduce())
        {
            return new ShardedAutoBatchReducer(index, ShardedMapReduceQueryResultsMerger.Aggregator, _context, _token);
        }

        throw new InvalidOperationException($"Index '{_queryStats.IndexName}' is not a map-reduce index");
    }

    private interface IShardedBatchReducer
    {
        BlittableJsonReaderObject ReduceBatch(List<BlittableJsonReaderObject> batch);
    }

    private class ShardedStaticBatchReducer : IShardedBatchReducer
    {
        private readonly IndexingFunc _reducingFunc;
        private readonly ReduceMapResultsOfStaticIndex.DynamicIterationOfAggregationBatchWrapper _wrapper;
        private readonly JsonOperationContext _context;
        private readonly IndexInformationHolder _index;

        // We reuse this list to avoid allocating a new List<object> for every single key
        private readonly List<object> _singleResultContainer = new(1);
        private IPropertyAccessor _propertyAccessor;

        public ShardedStaticBatchReducer(IndexInformationHolder index, JsonOperationContext context)
        {
            _index = index;
            _context = context;

            if (index is not StaticIndexInformationHolder staticIndex)
                throw new InvalidOperationException($"Index '{index.Name}' is not a static map-reduce index");

            _reducingFunc = staticIndex.Compiled.Reduce;

            // Initialize the wrapper once. We will just feed it new data in the loop.
            _wrapper = new ReduceMapResultsOfStaticIndex.DynamicIterationOfAggregationBatchWrapper();
        }

        public BlittableJsonReaderObject ReduceBatch(List<BlittableJsonReaderObject> batch)
        {
            _wrapper.InitializeForEnumeration(batch);

            _singleResultContainer.Clear();

            foreach (var output in _reducingFunc(_wrapper))
            {
                _singleResultContainer.Add(output);

                _propertyAccessor ??= PropertyAccessor.Create(output.GetType(), output);
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
    }

    private class ShardedAutoBatchReducer : IShardedBatchReducer
    {
        private readonly AutoMapReduceIndexDefinition _definition;
        private readonly ShardedAutoMapReduceIndexResultsAggregator _aggregator;
        private readonly JsonOperationContext _context;
        private readonly CancellationToken _token;

        private BlittableJsonReaderObject _reusableResultBuffer = null;

        public ShardedAutoBatchReducer(IndexInformationHolder index, ShardedAutoMapReduceIndexResultsAggregator aggregator, JsonOperationContext context, CancellationToken token)
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
            _aggregator.AggregateOn(batch, _definition, _context, null, ref _reusableResultBuffer, _token);
            return _reusableResultBuffer;
        }
    }
}
