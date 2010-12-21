//-----------------------------------------------------------------------
// <copyright file="AbstractViewGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
        private bool? containsProjection;

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

        protected IEnumerable<dynamic> Project(object self, Func<dynamic, dynamic> func)
        {
            if (self == null)
                yield break;
            if (self is IEnumerable == false || self is string)
                throw new InvalidOperationException("Attempted to enumerate over " + self.GetType().Name);

            foreach (var item in ((IEnumerable)self))
            {
                yield return func(item);
            }
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

        public virtual bool ContainsField(string field)
        {
            if (fields.Contains(field))
                return true;
            if (containsProjection == null)
            {
                containsProjection = ViewText != null && ViewText.Contains("Project(");
            }
            return containsProjection.Value;
        }
	}
}
