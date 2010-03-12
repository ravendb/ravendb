using System;

namespace Raven.Database.Linq
{
    public class AbstractViewGenerator
    {
        public IndexingFunc MapDefinition { get; set; }
        public IndexingFunc ReduceDefinition { get; set; }
        public string ViewText { get; set; }

        public string Trace(object o)
        {
            Console.WriteLine(o);
            return o.ToString();
        }
    }
}