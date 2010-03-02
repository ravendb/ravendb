using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Rhino.DivanDB.Json;

namespace Rhino.DivanDB.Linq
{
    public delegate IEnumerable IndexingFunc(IEnumerable<JsonDynamicObject> source);

    public class AbstractViewGenerator
    {
        public IndexingFunc CompiledDefinition { get; set; }
        public string ViewText { get; set; }

        public Expression<IndexingFunc> IndexDefinition { get; protected set; }

        public AbstractViewGenerator()
        {
            AccessedFields = new HashSet<string>();
        }

        public Type GeneratedType
        {
            get
            {
                return IndexDefinition.Body.Type.GetGenericArguments()[0];
            }
        }

        public IEnumerable Execute(IEnumerable<JsonDynamicObject> source)
        {
            return CompiledDefinition(source);
        }


        public HashSet<string> AccessedFields { get; set; }

    }
}