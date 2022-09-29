using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Includes.Sharding;

public class ShardedTimeSeriesIncludes : ITimeSeriesIncludes
{
    private Dictionary<string, BlittableJsonReaderObject> _resultsByDocumentId;

    public int Count => _resultsByDocumentId.Count;

    public void AddResults(BlittableJsonReaderObject results, JsonOperationContext contextToClone)
    {
        if (results == null)
            return;

        _resultsByDocumentId ??= new(StringComparer.OrdinalIgnoreCase);

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
        for (var i = 0; i < results.Count; i++)
        {
            results.GetPropertyByIndex(i, ref propertyDetails);

            string documentId = propertyDetails.Name;

            var json = (BlittableJsonReaderObject)propertyDetails.Value;

            var added = _resultsByDocumentId.TryAdd(documentId, json.Clone(contextToClone));

            if (added == false)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "We can get duplicated TS includes when resharding is running. How to deal with that?");
                throw new NotImplementedException("Handling of duplicated TS includes during resharding");
            }
        }
    }

    public async ValueTask<int> WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
    {
        int size = 0;
        writer.WriteStartObject();

        var first = true;
        foreach (var kvp in _resultsByDocumentId)
        {
            if (first == false)
                writer.WriteComma();

            first = false;

            writer.WritePropertyName(kvp.Key);
            writer.WriteObject(kvp.Value);

            size += kvp.Key.Length;
            size += kvp.Value.Size;

            await writer.MaybeFlushAsync(token);
        }

        writer.WriteEndObject();

        return size;
    }
}
