using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class ReduceMapResultsOfStaticIndex : ReduceMapResultsBase
    {
        private readonly DynamicIterationOfAggregationBatchWrapper _blittableToDynamicWrapper = new DynamicIterationOfAggregationBatchWrapper();
        private readonly IndexingFunc _reducingFunc;
        private PropertyAccessor _propertyAccessor;

        public ReduceMapResultsOfStaticIndex(IndexingFunc reducingFunc, IndexDefinitionBase indexDefinition, IndexStorage indexStorage, MetricsCountersManager metrics, MapReduceIndexingContext mapReduceContext)
            : base(indexDefinition, indexStorage, metrics, mapReduceContext)
        {
            _reducingFunc = reducingFunc;
        }
        
        protected override unsafe BlittableJsonReaderObject AggregateBatchResults(List<BlittableJsonReaderObject> aggregationBatch, long modifiedPage, 
                                                                                  int aggregatedEntries, Table table, TransactionOperationContext indexContext)
        {
            _blittableToDynamicWrapper.InitializeForEnumeration(aggregationBatch);

            var djv = new DynamicJsonValue();

            foreach (var output in _reducingFunc(_blittableToDynamicWrapper))
            {
                if (_propertyAccessor == null)
                    _propertyAccessor = PropertyAccessor.Create(output.GetType());

                foreach (var property in _propertyAccessor.Properties)
                {
                    djv[property.Key] = property.Value(output);
                }

                break;
            }

            aggregationBatch.Clear();

            var resultObj = indexContext.ReadObject(djv, "map/reduce");

            var pageNumber = IPAddress.HostToNetworkOrder(modifiedPage);

            table.Set(new TableValueBuilder
            {
                {(byte*)&pageNumber, sizeof(long)},
                {resultObj.BasePointer, resultObj.Size},
                {(byte*)&aggregatedEntries, sizeof(int)}
            });

            return resultObj;
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
                private readonly DynamicBlittableJson _dynamicJson = new DynamicBlittableJson(null);
                private IEnumerator<BlittableJsonReaderObject> _items;
                private BlittableJsonReaderObject _previous = null;

                public void Initialize(IEnumerator<BlittableJsonReaderObject> items)
                {
                    _items = items;
                }

                public bool MoveNext()
                {
                    if (_items.MoveNext() == false)
                        return false;

                    _previous?.Dispose();

                    _dynamicJson.Set(_items.Current);

                    Current = _dynamicJson;

                    _previous = _items.Current;

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
                    _previous?.Dispose();
                    _previous = null;
                }
            }
        }

    }
}