using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : MapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, HashSet<string> collections, string[] outputFields,
            string[] groupByFields, bool hasDynamicFields)
            : base(definition, collections, outputFields, hasDynamicFields)
        {
            GroupByFields = new HashSet<string>(groupByFields, StringComparer.Ordinal);
            OutputReduceToCollection = definition.OutputReduceToCollection;
        }

        public HashSet<string> GroupByFields { get; }
        public string OutputReduceToCollection { get; }
    }
}