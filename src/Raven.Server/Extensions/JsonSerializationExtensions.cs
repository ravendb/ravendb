using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Sparrow.Json.Parsing;

namespace Raven.Server.Extensions
{
    public static class JsonSerializationExtensions
    {
        public static DynamicJsonValue ToJson(this IQueryCollection queryCollection)
        {
            var jsonMap = new DynamicJsonValue();
            if (queryCollection == null) //precaution, prevent NRE
                return null;

            foreach (var kvp in queryCollection)
            {
                jsonMap[kvp.Key] = kvp.Value.ToArray();
            }

            return jsonMap;
        }
        
        public static DynamicJsonValue ToJson(this IndexDefinition definition)
        {
            var result = new DynamicJsonValue
            {
                [nameof(IndexDefinition.Etag)] = definition.Etag,
                [nameof(IndexDefinition.IsTestIndex)] = definition.IsTestIndex,
                [nameof(IndexDefinition.LockMode)] = definition.LockMode?.ToString(),
                [nameof(IndexDefinition.Priority)] = definition.Priority?.ToString(),
                [nameof(IndexDefinition.OutputReduceToCollection)] = definition.OutputReduceToCollection,
                [nameof(IndexDefinition.Name)] = definition.Name,
                [nameof(IndexDefinition.Reduce)] = definition.Reduce,
                [nameof(IndexDefinition.Type)] = definition.Type.ToString(),
                [nameof(IndexDefinition.Maps)] = new DynamicJsonArray(definition.Maps)
            };

            var fields = new DynamicJsonValue();
            foreach (var kvp in definition.Fields)
            {
                DynamicJsonValue spatial = null;
                if (kvp.Value.Spatial != null)
                {
                    spatial = new DynamicJsonValue
                    {
                        [nameof(SpatialOptions.MaxTreeLevel)] = kvp.Value.Spatial.MaxTreeLevel,
                        [nameof(SpatialOptions.MaxX)] = kvp.Value.Spatial.MaxX,
                        [nameof(SpatialOptions.MaxY)] = kvp.Value.Spatial.MaxY,
                        [nameof(SpatialOptions.MinX)] = kvp.Value.Spatial.MinX,
                        [nameof(SpatialOptions.MinY)] = kvp.Value.Spatial.MinY,
                        [nameof(SpatialOptions.Strategy)] = kvp.Value.Spatial.Strategy.ToString(),
                        [nameof(SpatialOptions.Type)] = kvp.Value.Spatial.Type.ToString(),
                        [nameof(SpatialOptions.Units)] = kvp.Value.Spatial.Units.ToString()
                    };
                }

                var field = new DynamicJsonValue
                {
                    [nameof(IndexFieldOptions.Analyzer)] = kvp.Value.Analyzer,
                    [nameof(IndexFieldOptions.Indexing)] = kvp.Value.Indexing?.ToString(),
                    [nameof(IndexFieldOptions.Spatial)] = spatial,
                    [nameof(IndexFieldOptions.Storage)] = kvp.Value.Storage?.ToString(),
                    [nameof(IndexFieldOptions.Suggestions)] = kvp.Value.Suggestions,
                    [nameof(IndexFieldOptions.TermVector)] = kvp.Value.TermVector?.ToString()
                };

                fields[kvp.Key] = field;
            }

            result[nameof(IndexDefinition.Fields)] = fields;

            var settings = new DynamicJsonValue();
            foreach (var kvp in definition.Configuration)
                settings[kvp.Key] = kvp.Value;

            result[nameof(IndexDefinition.Configuration)] = settings;

            var additionalSources = new DynamicJsonValue();
            foreach (var kvp in definition.AdditionalSources)
                additionalSources[kvp.Key] = kvp.Value;

            result[nameof(IndexDefinition.AdditionalSources)] = additionalSources;

            return result;
        }
    }
}
