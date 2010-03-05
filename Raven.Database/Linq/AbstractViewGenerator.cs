using System.Collections.Generic;

namespace Raven.Database.Linq
{
    public delegate IEnumerable<dynamic> IndexingFunc(IEnumerable<dynamic> source);

    public class AbstractViewGenerator
    {
        public IndexingFunc CompiledDefinition { get; set; }
        public string ViewText { get; set; }

        public IEnumerable<dynamic> Execute(IEnumerable<dynamic> source)
        {
            return CompiledDefinition(source);
        }

        protected IEnumerable<dynamic> Array(object[] self)
        {
            return self;
        }
    }
}