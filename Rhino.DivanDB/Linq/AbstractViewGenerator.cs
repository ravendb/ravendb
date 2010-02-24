using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Json;

namespace Rhino.DivanDB.Linq
{
    public delegate IEnumerable ViewFunc(IEnumerable<JsonDynamicObject> source);

    public class AbstractViewGenerator
    {
        private ViewFunc compiledDefinition;
        public string ViewText { get; set; }

        public Expression<ViewFunc> ViewDefinition { get; protected set; }

        public Type GeneratedType
        {
            get
            {
                return ViewDefinition.Body.Type.GetGenericArguments()[0];
            }
        }

        public IEnumerable Execute(IEnumerable<JsonDynamicObject> source)
        {
            ForceCompilationIfNeeded();
            return compiledDefinition(source);
        }

        public ViewFunc CompiledDefinition
        {
            get
            {
                ForceCompilationIfNeeded();
                return compiledDefinition;
            }
        }

        private void ForceCompilationIfNeeded()
        {
            if (compiledDefinition == null)
            {
                var def = ViewDefinition.Compile();
                compiledDefinition = source => def(AddViewContextCurrentDocumentId(source));
            }
        }

        private static IEnumerable<JsonDynamicObject> AddViewContextCurrentDocumentId(IEnumerable<JsonDynamicObject> source)
        {
            foreach (var doc in source)
            {
                ViewContext.CurrentDocumentId = doc["_id"].Unwrap();
                yield return doc;
            }
            ViewContext.CurrentDocumentId = null;

        }
    }
}