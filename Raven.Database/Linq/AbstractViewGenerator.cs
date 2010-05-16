using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Database.Indexing;

namespace Raven.Database.Linq
{
    [InheritedExport]
	public abstract class AbstractViewGenerator
	{
		public IndexingFunc MapDefinition { get; set; }
		
        public IndexingFunc ReduceDefinition { get; set; }
		
        public GroupByKeyFunc GroupByExtraction { get; set; }
        
        public string ViewText { get; set; }
        
        public IDictionary<string, FieldStorage> Stores { get; set; }
        
        public IDictionary<string, FieldIndexing> Indexes { get; set; }

        protected AbstractViewGenerator()
        {
            Stores = new Dictionary<string, FieldStorage>();
            Indexes = new Dictionary<string, FieldIndexing>();
        }
	}
}