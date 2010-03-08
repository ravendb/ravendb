namespace Raven.Database.Linq
{
    public class AbstractViewGenerator
    {
        public IndexingFunc MapDefinition { get; set; }
        public IndexingFunc ReduceDefinition { get; set; }
        public string ViewText { get; set; }
    }
}