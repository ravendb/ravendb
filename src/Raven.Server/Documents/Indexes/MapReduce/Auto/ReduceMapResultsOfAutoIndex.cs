using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public unsafe class ReduceMapResultsOfAutoIndex : ReduceMapResultsBase<AutoMapReduceIndexDefinition>
    {
        public ReduceMapResultsOfAutoIndex(AutoMapReduceIndexDefinition indexDefinition, IndexStorage indexStorage,
            MetricsCountersManager metrics, MapReduceIndexingContext mapReduceContext)
            : base(indexDefinition, indexStorage, metrics, mapReduceContext)
        {
        }

        protected override AggregationResult AggregateOn(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, CancellationToken token)
        {
            var aggregatedResultsByReduceKey = new Dictionary<BlittableJsonReaderObject, Dictionary<string, PropertyResult>>(ReduceKeyComparer.Instance);

            foreach (var obj in aggregationBatch)
            {
                token.ThrowIfCancellationRequested();

                using (obj)
                {
                    var aggregatedResult = new Dictionary<string, PropertyResult>();

                    foreach (var propertyName in obj.GetPropertyNames())
                    {
                        string stringValue;

                        IndexField indexField;
                        if (_indexDefinition.TryGetField(propertyName, out indexField))
                        {
                            switch (indexField.MapReduceOperation)
                            {
                                case FieldMapReduceOperation.Count:
                                case FieldMapReduceOperation.Sum:

                                    object value;
                                    if (obj.TryGetMember(propertyName, out value) == false)
                                        throw new InvalidOperationException($"Could not read numeric value of '{propertyName}' property");

                                    double doubleValue;
                                    long longValue;

                                    var numberType = BlittableNumber.Parse(value, out doubleValue, out longValue);
                                    
                                    var aggregate = new PropertyResult(numberType);

                                    switch (numberType)
                                    {
                                        case NumberParseResult.Double:
                                            aggregate.ResultValue = aggregate.DoubleValue = doubleValue;
                                            break;
                                        case NumberParseResult.Long:
                                            aggregate.ResultValue = aggregate.LongValue = longValue;
                                            break;
                                        default:
                                            throw new ArgumentOutOfRangeException($"Unknown number type: {numberType}");
                                    }

                                    aggregatedResult[propertyName] = aggregate;
                                    break;
                                //case FieldMapReduceOperation.None:
                                default:
                                    throw new ArgumentOutOfRangeException($"Unhandled field type '{indexField.MapReduceOperation}' to aggregate on");
                            }
                        }
                        else if (obj.TryGet(propertyName, out stringValue))
                        {
                            aggregatedResult[propertyName] = new PropertyResult
                            {
                                ResultValue = stringValue
                            };
                        }

                        if (_indexDefinition.ContainsGroupByField(propertyName) == false)
                        {
                            // we want to reuse existing entry to get a reduce key

                            if (obj.Modifications == null)
                                obj.Modifications = new DynamicJsonValue(obj);

                            obj.Modifications.Remove(propertyName);
                        }
                    }

                    var reduceKey = indexContext.ReadObject(obj, "reduce key");

                    Dictionary<string, PropertyResult> existingAggregate;
                    if (aggregatedResultsByReduceKey.TryGetValue(reduceKey, out existingAggregate) == false)
                    {
                        aggregatedResultsByReduceKey.Add(reduceKey, aggregatedResult);
                    }
                    else
                    {
                        reduceKey.Dispose();

                        foreach (var propertyResult in existingAggregate)
                        {
                            propertyResult.Value.Aggregate(aggregatedResult[propertyResult.Key]);
                        }
                    }
                }
            }
            
            var resultObjects = new List<BlittableJsonReaderObject>(aggregatedResultsByReduceKey.Count);

            foreach (var aggregationResult in aggregatedResultsByReduceKey)
            {
                aggregationResult.Key.Dispose();

                var djv = new DynamicJsonValue();

                foreach (var aggregate in aggregationResult.Value)
                {
                    djv[aggregate.Key] = aggregate.Value.ResultValue;
                }

                resultObjects.Add(indexContext.ReadObject(djv, "map/reduce"));
            }
            
            return new AggregationResult(resultObjects);
        }

        private class PropertyResult
        {
            private readonly NumberParseResult? _numberType;

            public object ResultValue;

            public long LongValue = 0;

            public double DoubleValue = 0;

            public PropertyResult(NumberParseResult? numberType = null)
            {
                _numberType = numberType;
            }

            public void Aggregate(PropertyResult other)
            {
                if (_numberType != null)
                {
                    switch (_numberType.Value)
                    {
                        case NumberParseResult.Double:
                            ResultValue = DoubleValue += other.DoubleValue;
                            break;
                        case NumberParseResult.Long:
                            ResultValue = LongValue += other.LongValue;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"Unknown number type: {_numberType.Value}");
                    }
                } 
            }
        }

        private class ReduceKeyComparer : IEqualityComparer<BlittableJsonReaderObject>
        {
            public static readonly ReduceKeyComparer Instance = new ReduceKeyComparer();

            public bool Equals(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
            {
                if (x.Size != y.Size)
                    return false;

                return Memory.Compare(x.BasePointer, y.BasePointer, x.Size) == 0;
            }

            public int GetHashCode(BlittableJsonReaderObject obj)
            {
                return 1; // calculated hash of a reduce key is the same for all entries in a tree, we have to force Equals method to be called
            }
        }
    }
}