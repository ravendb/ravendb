using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptMapReduceIndex: MapReduceIndex
    {
        protected JavaScriptMapReduceIndex(MapReduceIndexDefinition definition, StaticIndexBase compiled) : base(definition, compiled)
        {
        }

        public new static Index CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            return MapReduceIndex.CreateNew(definition, documentDatabase);
        }
    }
}
