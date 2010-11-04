using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Database.Indexing;

namespace Raven.Database.Linq
{
    [InheritedExport]
	public abstract class AbstractViewGenerator
	{
        private readonly HashSet<string> fields = new HashSet<string>();

		public IndexingFunc MapDefinition { get; set; }
		
        public IndexingFunc ReduceDefinition { get; set; }

        public TranslatorFunc TransformResultsDefinition { get; set; }
		
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

        public void AddField(string field)
        {
            fields.Add(field);
        }

        public bool ContainsField(string field)
        {
            return fields.Contains(field);
        }
	}
}
