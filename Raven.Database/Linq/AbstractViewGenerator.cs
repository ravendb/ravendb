//-----------------------------------------------------------------------
// <copyright file="AbstractViewGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Indexing;
using System.Linq;

namespace Raven.Database.Linq
{
	/// <summary>
	/// This class represents a base class for all "Views" we generate and compile on the fly - all
	/// Map and MapReduce indexes are being re-written into this class and then compiled and executed
	/// against the data in RavenDB
	/// </summary>
    [InheritedExport]
	public abstract class AbstractViewGenerator
	{
        private readonly HashSet<string> fields = new HashSet<string>();
        private bool? containsProjection;
		private readonly HashSet<string> mapFields = new HashSet<string>();
		private readonly HashSet<string> reduceFields = new HashSet<string>();

    	public int CountOfFields { get { return fields.Count;  } }

    	public IndexingFunc MapDefinition { get; set; }
		
        public IndexingFunc ReduceDefinition { get; set; }

        public TranslatorFunc TransformResultsDefinition { get; set; }
		
        public GroupByKeyFunc GroupByExtraction { get; set; }
        
        public string ViewText { get; set; }
        
        public IDictionary<string, FieldStorage> Stores { get; set; }
        
        public IDictionary<string, FieldIndexing> Indexes { get; set; }

    	public string ForEntityName { get; set; }

    	public string[] Fields
    	{
    		get { return fields.ToArray(); }
    	}

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

		public void AddQueryParameterForMap(string field)
		{
			mapFields.Add(field);
		}

    	public void AddQueryParameterForReduce(string field)
    	{
    		reduceFields.Add(field);
    	}

        public void AddField(string field)
        {
            fields.Add(field);
        }

		public virtual bool ContainsFieldOnMap(string field)
		{
			return mapFields.Contains(field);
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
