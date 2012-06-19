//-----------------------------------------------------------------------
// <copyright file="AbstractViewGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using Lucene.Net.Documents;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using System.Linq;
using Raven.Database.Indexing;
using Raven.Json.Linq;

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
		private int? countOfSelectMany;
		private bool? hasWhereClause;
		private readonly HashSet<string> mapFields = new HashSet<string>();
		private readonly HashSet<string> reduceFields = new HashSet<string>();

		private static readonly Regex selectManyOrFrom = new Regex(@"( (?<!^)\s from \s ) | ( \.SelectMany\( )", 
			RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		public string SourceCode { get; set; }

		public int CountOfSelectMany
		{
			get
			{
				if(countOfSelectMany == null)
				{
					countOfSelectMany = selectManyOrFrom.Matches(ViewText).Count;
				}
				return countOfSelectMany.Value;
			}
		}

		public int CountOfFields { get { return fields.Count;  } }

		public List<IndexingFunc> MapDefinitions { get; private set; }
		
		public IndexingFunc ReduceDefinition { get; set; }

		public TranslatorFunc TransformResultsDefinition { get; set; }
		
		public GroupByKeyFunc GroupByExtraction { get; set; }
		
		public string ViewText { get; set; }
		
		public IDictionary<string, FieldStorage> Stores { get; set; }
		
		public IDictionary<string, FieldIndexing> Indexes { get; set; }

		public HashSet<string> ForEntityNames { get; set; }

		public string[] Fields
		{
			get { return fields.ToArray(); }
		}

		public bool HasWhereClause
		{
			get
			{
				if(hasWhereClause == null)
				{
					hasWhereClause = ViewText.IndexOf("where", StringComparison.OrdinalIgnoreCase) > -1;
				}
				return hasWhereClause.Value;
			}
		}

		protected AbstractViewGenerator()
		{
			MapDefinitions = new List<IndexingFunc>();
			ForEntityNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			Stores = new Dictionary<string, FieldStorage>();
			Indexes = new Dictionary<string, FieldIndexing>();
		}

		protected IEnumerable<AbstractField> CreateField(string name, object value, bool stored = false, bool analyzed = true)
		{
			var indexDefinition = new IndexDefinition();
			indexDefinition.Indexes[name] = analyzed ? FieldIndexing.Analyzed : FieldIndexing.NotAnalyzed;
			var anonymousObjectToLuceneDocumentConverter = new AnonymousObjectToLuceneDocumentConverter(indexDefinition);

			return anonymousObjectToLuceneDocumentConverter.CreateFields(name, value, stored ? Field.Store.YES : Field.Store.NO);
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
			if (field.EndsWith("_Range")) field = field.Substring(0, field.Length - 6);
			if (ReduceDefinition == null)
				return fields.Contains(field);
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

		protected void AddMapDefinition(IndexingFunc mapDef)
		{
			MapDefinitions.Add(mapDef);
		}
		
		protected IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
		{
			return new RecursiveFunction(item, func).Execute();
		}
	}
}
