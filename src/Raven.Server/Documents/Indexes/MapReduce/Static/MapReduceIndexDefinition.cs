using System;
using System.Collections.Generic;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : MapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, string[] collections, string[] outputFields,
            string[] groupByFields, bool hasDynamicFields, string outputReduceResultsToCollectionName)
            : base(definition, collections, outputFields, hasDynamicFields)
        {
            GroupByFields = new HashSet<string>(groupByFields, StringComparer.Ordinal);
            OutputReduceResultsToCollectionName = outputReduceResultsToCollectionName;
        }

        public HashSet<string> GroupByFields { get; private set; }
        public HashSet<string> GroupByFieldsa { get; private set; }
        public string OutputReduceResultsToCollectionName { get; private set; }
    }
}