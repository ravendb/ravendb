using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Sparrow.Json.Parsing;

namespace Raven.Server.Extensions
{
    public static class JsonSerializationExtensions
    {
        public static DynamicJsonValue ToJson(this IndexDefinition definition)
        {
            var result = new DynamicJsonValue();
#if FEATURE_TEST_INDEX
            result[nameof(IndexDefinition.IsTestIndex)] = definition.IsTestIndex;
#endif
            result[nameof(IndexDefinition.SourceType)] = definition.SourceType.ToString();
            result[nameof(IndexDefinition.LockMode)] = definition.LockMode?.ToString();
            result[nameof(IndexDefinition.Priority)] = definition.Priority?.ToString();
            result[nameof(IndexDefinition.State)] = definition.State?.ToString();
            result[nameof(IndexDefinition.OutputReduceToCollection)] = definition.OutputReduceToCollection;
            result[nameof(IndexDefinition.PatternForOutputReduceToCollectionReferences)] = definition.PatternForOutputReduceToCollectionReferences;
            result[nameof(IndexDefinition.PatternReferencesCollectionName)] = definition.PatternReferencesCollectionName;
            result[nameof(IndexDefinition.ReduceOutputIndex)] = definition.ReduceOutputIndex;
            result[nameof(IndexDefinition.Name)] = definition.Name;
            result[nameof(IndexDefinition.Reduce)] = definition.Reduce;
            result[nameof(IndexDefinition.Type)] = definition.Type.ToString();
            result[nameof(IndexDefinition.Maps)] = new DynamicJsonArray(definition.Maps);

            var fields = new DynamicJsonValue();
            foreach (var kvp in definition.Fields)
            {
                DynamicJsonValue spatial = null;
                if (kvp.Value.Spatial != null)
                {
                    spatial = new DynamicJsonValue();
                    spatial[nameof(SpatialOptions.MaxTreeLevel)] = kvp.Value.Spatial.MaxTreeLevel;
                    spatial[nameof(SpatialOptions.MaxX)] = kvp.Value.Spatial.MaxX;
                    spatial[nameof(SpatialOptions.MaxY)] = kvp.Value.Spatial.MaxY;
                    spatial[nameof(SpatialOptions.MinX)] = kvp.Value.Spatial.MinX;
                    spatial[nameof(SpatialOptions.MinY)] = kvp.Value.Spatial.MinY;
                    spatial[nameof(SpatialOptions.Strategy)] = kvp.Value.Spatial.Strategy.ToString();
                    spatial[nameof(SpatialOptions.Type)] = kvp.Value.Spatial.Type.ToString();
                    spatial[nameof(SpatialOptions.Units)] = kvp.Value.Spatial.Units.ToString();
                }

                var field = new DynamicJsonValue();
                field[nameof(IndexFieldOptions.Analyzer)] = kvp.Value.Analyzer;
                field[nameof(IndexFieldOptions.Indexing)] = kvp.Value.Indexing?.ToString();
                field[nameof(IndexFieldOptions.Spatial)] = spatial;
                field[nameof(IndexFieldOptions.Storage)] = kvp.Value.Storage?.ToString();
                field[nameof(IndexFieldOptions.Suggestions)] = kvp.Value.Suggestions;
                field[nameof(IndexFieldOptions.TermVector)] = kvp.Value.TermVector?.ToString();

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
