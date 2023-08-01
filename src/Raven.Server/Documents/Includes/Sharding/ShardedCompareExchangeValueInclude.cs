using System.Collections.Generic;
using System;
using Raven.Client.Documents.Operations.CompareExchange;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class ShardedCompareExchangeValueInclude : ICompareExchangeValueIncludes
{
    public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results { get; set; }

    public void AddResults(BlittableJsonReaderObject results, JsonOperationContext contextToClone)
    {
        if (results == null)
            return;

        Results ??= new Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
        for (var i = 0; i < results.Count; i++)
        {
            results.GetPropertyByIndex(i, ref propertyDetails);

            var value = CompareExchangeValue<BlittableJsonReaderObject>.CreateFrom(propertyDetails.Value as BlittableJsonReaderObject);

            if (Results.TryGetValue(value.Key, out var existing) && existing.Index > value.Index)
                continue; // always pick newest

            if (value.Value != null)
                value.Value = value.Value.Clone(contextToClone);

            Results[value.Key] = value;
        }
    }
}
