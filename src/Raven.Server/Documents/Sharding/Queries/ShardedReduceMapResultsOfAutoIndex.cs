using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Queries;

public sealed class ShardedAutoMapReduceIndexResultsAggregatorForIndexEntries : AutoMapReduceIndexResultsAggregator
{
    private string _reduceKeyHash;

    internal override void HandleProperty(AutoMapReduceIndexDefinition indexDefinition, string propertyName, BlittableJsonReaderObject json, Dictionary<string, PropertyResult> aggregatedResult)
    {
        if (_reduceKeyHash == null && propertyName == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName)
            json.TryGet(propertyName, out _reduceKeyHash);

        base.HandleProperty(indexDefinition, propertyName, json, aggregatedResult);
    }

    internal override DynamicJsonValue BuildResult(KeyValuePair<BlittableJsonReaderObject, Dictionary<string, PropertyResult>> aggregationResult)
    {
        var djv = base.BuildResult(aggregationResult);

        if (_reduceKeyHash != null)
            djv[Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName] = _reduceKeyHash;

        return djv;
    }

    internal override PropertyResult HandleSumAndCount(object value)
    {
        if (value is LazyStringValue or LazyCompressedStringValue)
        {
            var valueAsString = value.ToString();
            if (long.TryParse(valueAsString, out long lngVal))
                value = lngVal;
            else if (double.TryParse(valueAsString, out double dblVal))
                value = dblVal;
        }

        return base.HandleSumAndCount(value);
    }
}
