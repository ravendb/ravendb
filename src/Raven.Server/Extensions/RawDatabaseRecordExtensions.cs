using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Extensions;

public static class RawDatabaseRecordExtensions
{
    public static Dictionary<string, IndexDefinition> MapReduceIndexes(this RawDatabaseRecord record)
    {
        var mapReduceIndexes = new Dictionary<string, IndexDefinition>(StringComparer.OrdinalIgnoreCase);

        if (record.Raw.TryGet(nameof(DatabaseRecord.Indexes), out BlittableJsonReaderObject obj) == false || obj == null)
            return mapReduceIndexes;

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
        for (var i = 0; i < obj.Count; i++)
        {
            obj.GetPropertyByIndex(i, ref propertyDetails);

            if (propertyDetails.Value == null)
                continue;

            if (propertyDetails.Value is BlittableJsonReaderObject bjro == false)
                continue;

            if (bjro.TryGet(nameof(IndexDefinition.Type), out IndexType indexType) == false ||
                indexType.IsStaticMapReduce() == false)
                continue;

            mapReduceIndexes[propertyDetails.Name] = JsonDeserializationCluster.IndexDefinition(bjro);
        }

        return mapReduceIndexes;
    }

    public static Dictionary<string, AutoIndexDefinition> AutoMapReduceIndexes(this RawDatabaseRecord record)
    {
        var autoMapReduceIndexes = new Dictionary<string, AutoIndexDefinition>(StringComparer.OrdinalIgnoreCase);

        if (record.Raw.TryGet(nameof(DatabaseRecord.AutoIndexes), out BlittableJsonReaderObject obj) == false || obj == null)
            return autoMapReduceIndexes;

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
        for (var i = 0; i < obj.Count; i++)
        {
            obj.GetPropertyByIndex(i, ref propertyDetails);

            if (propertyDetails.Value == null)
                continue;

            if (propertyDetails.Value is BlittableJsonReaderObject bjro == false)
                continue;

            if (bjro.TryGet(nameof(IndexDefinition.Type), out IndexType indexType) == false ||
                indexType.IsAutoMapReduce() == false)
                continue;

            autoMapReduceIndexes[propertyDetails.Name] = JsonDeserializationCluster.AutoIndexDefinition(bjro);
        }

        return autoMapReduceIndexes;
    }
}
