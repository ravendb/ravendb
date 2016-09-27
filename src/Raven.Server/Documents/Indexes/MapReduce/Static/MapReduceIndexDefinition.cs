using System;
using System.Collections.Generic;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : StaticMapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, string[] collections, string[] outputFields, string[] groupByFields, bool hasDynamicFields)
            : base(definition, collections, outputFields, hasDynamicFields)
        {
            GroupByFields = new HashSet<string>(groupByFields, StringComparer.Ordinal);
        }

        public HashSet<string> GroupByFields { get; private set; }
    }
}