using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Database.Indexing;
using Raven.Database.Linq.PrivateExtensions;

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

    	public string ForEntityName { get; set; }

    	protected AbstractViewGenerator()
        {
            Stores = new Dictionary<string, FieldStorage>();
            Indexes = new Dictionary<string, FieldIndexing>();
        }

		protected IEnumerable<dynamic> Hierarchy(object source, string name)
		{
			var djo = (DynamicJsonObject)source;
			foreach (var item in ((IEnumerable)djo.GetValue(name)))
			{
				yield return item;
				foreach (var subItem in Hierarchy(item, name))
				{
					yield return subItem;
				}
			}
		}
	}
}