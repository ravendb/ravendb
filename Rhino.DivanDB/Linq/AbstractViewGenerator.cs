using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Rhino.DivanDB.Json;

namespace Rhino.DivanDB.Linq
{
    public delegate IEnumerable ViewFunc(IEnumerable<JsonDynamicObject> source);

    public class AbstractViewGenerator
    {
        private ViewFunc compiledDefinition;
        public string ViewText { get; set; }

        public Expression<ViewFunc> ViewDefinition { get; protected set; }

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
                compiledDefinition = ViewDefinition.Compile();
        }
    }
}