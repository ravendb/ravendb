using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Rhino.DivanDB.Linq
{
    public class AbstractViewGenerator<TSource>
    {
        private Func<IEnumerable<TSource>, IEnumerable> compiledDefinition;
        public string ViewText { get; set; }
        public Expression<Func<IEnumerable<TSource>,IEnumerable>>  ViewDefinition { get; set; }

        public IEnumerable Execute(IEnumerable<TSource> source)
        {
            if (compiledDefinition == null)
                compiledDefinition = ViewDefinition.Compile();
            return compiledDefinition(source);
        }
    }
}