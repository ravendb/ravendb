using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Indexing;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public unsafe class ReduceMapResultsOfAutoIndex : ReduceMapResultsBase
    {
        public ReduceMapResultsOfAutoIndex(IndexDefinitionBase indexDefinition, IndexStorage indexStorage,
            MetricsCountersManager metrics, MapReduceIndexingContext mapReduceContext)
            : base(indexDefinition, indexStorage, metrics, mapReduceContext)
        {
        }

        protected override BlittableJsonReaderObject AggregateBatchResults(List<BlittableJsonReaderObject> aggregationBatch, long modifiedPage, int aggregatedEntries, 
                                                                           Table table, TransactionOperationContext indexContext)
        {
            var aggregatedResult = new Dictionary<string, PropertyResult>();

            foreach (var obj in aggregationBatch)
            {
                using (obj)
                {
                    foreach (var propertyName in obj.GetPropertyNames())
                    {
                        string stringValue;

                        IndexField indexField;
                        if (_indexDefinition.MapFields.TryGetValue(propertyName, out indexField))
                        {
                            switch (indexField.MapReduceOperation)
                            {
                                case FieldMapReduceOperation.Count:
                                case FieldMapReduceOperation.Sum:
                                    object value;

                                    if (obj.TryGetMember(propertyName, out value) == false)
                                        throw new InvalidOperationException(
                                            $"Could not read numeric value of '{propertyName}' property");

                                    double doubleValue;
                                    long longValue;

                                    var numberType = BlittableNumber.Parse(value, out doubleValue, out longValue);

                                    PropertyResult aggregate;
                                    if (aggregatedResult.TryGetValue(propertyName, out aggregate) == false)
                                    {
                                        var propertyResult = new PropertyResult();

                                        switch (numberType)
                                        {
                                            case NumberParseResult.Double:
                                                propertyResult.ResultValue = doubleValue;
                                                propertyResult.DoubleSumValue = doubleValue;
                                                break;
                                            case NumberParseResult.Long:
                                                propertyResult.ResultValue = longValue;
                                                propertyResult.LongSumValue = longValue;
                                                break;
                                        }

                                        aggregatedResult[propertyName] = propertyResult;
                                    }
                                    else
                                    {
                                        switch (numberType)
                                        {
                                            case NumberParseResult.Double:
                                                aggregate.ResultValue = aggregate.DoubleSumValue += doubleValue;
                                                break;
                                            case NumberParseResult.Long:
                                                aggregate.ResultValue = aggregate.LongSumValue += longValue;
                                                break;
                                        }
                                        ;
                                    }
                                    break;
                                //case FieldMapReduceOperation.None:
                                default:
                                    throw new ArgumentOutOfRangeException(
                                        $"Unhandled field type '{indexField.MapReduceOperation}' to aggregate on");
                            }
                        }
                        else if (obj.TryGet(propertyName, out stringValue))
                        {
                            if (aggregatedResult.ContainsKey(propertyName) == false)
                            {
                                aggregatedResult[propertyName] = new PropertyResult
                                {
                                    ResultValue = stringValue
                                };
                            }
                        }
                    }
                }
            }

            aggregationBatch.Clear();

            var djv = new DynamicJsonValue();

            foreach (var aggregate in aggregatedResult)
            {
                djv[aggregate.Key] = aggregate.Value.ResultValue;
            }

            var resultObj = indexContext.ReadObject(djv, "map/reduce");

            var pageNumber = IPAddress.HostToNetworkOrder(modifiedPage);

            table.Set(new TableValueBuilder
                {
                    {(byte*) &pageNumber, sizeof (long)},
                    {resultObj.BasePointer, resultObj.Size},
                    {(byte*) &aggregatedEntries, sizeof (int)}
                });

            return resultObj;
        }

        private class PropertyResult
        {
            public object ResultValue;

            public long LongSumValue = 0;

            public double DoubleSumValue = 0;
        }
    }
}