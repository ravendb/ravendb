using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptMapIndex: MapIndex
    {
        protected JavaScriptMapIndex(MapIndexDefinition definition, StaticIndexBase compiled) : base(definition, compiled)
        {
            
        }

        //TODO:we might want to modify this method in the future for now its redundent
        public new static Index CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            return MapIndex.CreateNew(definition, documentDatabase);
        }
    }
}
