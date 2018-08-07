using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using DynamicBlittableJson = Raven.Server.Documents.Indexes.Static.DynamicBlittableJson;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class ReduceMapResultsOfStaticIndex : ReduceMapResultsBase<MapReduceIndexDefinition>
    {
        private readonly DynamicIterationOfAggregationBatchWrapper _blittableToDynamicWrapper = new DynamicIterationOfAggregationBatchWrapper();
        private readonly IndexingFunc _reducingFunc;
        private PropertyAccessor _propertyAccessor;

        public ReduceMapResultsOfStaticIndex(Index index, IndexingFunc reducingFunc, MapReduceIndexDefinition indexDefinition, IndexStorage indexStorage, MetricCounters metrics, MapReduceIndexingContext mapReduceContext)
            : base(index, indexDefinition, indexStorage, metrics, mapReduceContext)
        {
            _reducingFunc = reducingFunc;
        }
        
        protected override AggregationResult AggregateOn(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, CancellationToken token)
        {
            _blittableToDynamicWrapper.InitializeForEnumeration(aggregationBatch);
            
            var resultObjects = new List<object>();

            foreach (var output in _reducingFunc(_blittableToDynamicWrapper))
            {
                token.ThrowIfCancellationRequested();

                if (_propertyAccessor == null)
                    _propertyAccessor = PropertyAccessor.Create(output.GetType(), output);

                resultObjects.Add(output);
            }

            return new AggregatedAnonymousObjects(resultObjects, _propertyAccessor, indexContext);
        }

        private class DynamicIterationOfAggregationBatchWrapper : IEnumerable<DynamicBlittableJson>
        {
            private readonly Enumerator _enumerator = new Enumerator();

            public void InitializeForEnumeration(List<BlittableJsonReaderObject> aggregationBatch)
            {
                _enumerator.Initialize(aggregationBatch.GetEnumerator());
            }

            public IEnumerator<DynamicBlittableJson> GetEnumerator()
            {
                return _enumerator;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<DynamicBlittableJson>
            {
                private IEnumerator<BlittableJsonReaderObject> _items;

                public void Initialize(IEnumerator<BlittableJsonReaderObject> items)
                {
                    _items = items;
                }

                public bool MoveNext()
                {
                    if (_items.MoveNext() == false)
                        return false;

                    Current = new DynamicBlittableJson(_items.Current); // we have to create new instance to properly GroupBy

                    return true;
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public DynamicBlittableJson Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }
        }

    }
}
