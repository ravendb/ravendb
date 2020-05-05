using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : MapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, IEnumerable<string> collections, string[] outputFields, CompiledIndexField[] groupByFields, bool hasDynamicFields, bool hasCompareExchange, long indexVersion)
            : base(definition, collections, outputFields, hasDynamicFields, hasCompareExchange, indexVersion)
        {
            GroupByFields = new HashSet<CompiledIndexField>(groupByFields);
            OutputReduceToCollection = definition.OutputReduceToCollection;
            ReduceOutputIndex = definition.ReduceOutputIndex;
            PatternForOutputReduceToCollectionReferences = definition.PatternForOutputReduceToCollectionReferences;
            PatternReferencesCollectionName = definition.PatternReferencesCollectionName;
        }

        public HashSet<CompiledIndexField> GroupByFields { get; }
        public string OutputReduceToCollection { get; }
        public long? ReduceOutputIndex { get; set; }
        public string PatternForOutputReduceToCollectionReferences { get; }
        public string PatternReferencesCollectionName { get; }
    }
}
