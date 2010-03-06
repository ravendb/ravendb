using System.Collections.Generic;

namespace Raven.Database.Linq
{
    /// <summary>
    ///   Abstract class to use for the compilation of all the indexes
    /// </summary>
    public class AbstractViewGenerator
    {
        public IndexingFunc CompiledDefinition { get; set; }
        public string ViewText { get; set; }

        public IEnumerable<dynamic> Execute(IEnumerable<dynamic> source)
        {
            return CompiledDefinition(source);
        }
    }
}