//-----------------------------------------------------------------------
// <copyright file="AbstractViewGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text.RegularExpressions;
using Lucene.Net.Documents;
using Lucene.Net.Spatial;
using Lucene.Net.Spatial.Prefix;
using Lucene.Net.Spatial.Prefix.Tree;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using System.Linq;
using Raven.Database.Indexing;
using Spatial4n.Core.Shapes;

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
		private IndexDefinition indexDefinition;

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
			SpatialStrategies = new ConcurrentDictionary<string, SpatialStrategy>();
		}

		public void Init(IndexDefinition definition)
		{
			indexDefinition = definition;
		}

		protected IEnumerable<AbstractField> CreateField(string name, object value, bool stored = false, bool analyzed = true)
		{
			return new AnonymousObjectToLuceneDocumentConverter(indexDefinition)
				.CreateFields(name, value, stored ? Field.Store.YES : Field.Store.NO);
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

		#region Spatial index

		[CLSCompliant(false)]
		public ConcurrentDictionary<string, SpatialStrategy> SpatialStrategies { get; private set; }

		public IEnumerable<IFieldable> SpatialGenerate(double? lat, double? lng)
		{
			return SpatialGenerate(Constants.DefaultSpatialFieldName, lat, lng);
		}

		public IEnumerable<IFieldable> SpatialGenerate(string fieldName, double? lat, double? lng)
		{
			var strategy = SpatialStrategies.GetOrAdd(fieldName, s => SpatialIndex.CreateStrategy(fieldName, SpatialSearchStrategy.GeohashPrefixTree,
				GeohashPrefixTree.GetMaxLevelsPossible()));

// ReSharper disable CSharpWarnings::CS0612
			Shape shape = SpatialIndex.Context.MakePoint(lng ?? 0, lat ?? 0);
			return strategy.CreateIndexableFields(shape)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, SpatialIndex.ShapeReadWriter.WriteShape(shape), Field.Store.YES, Field.Index.NO), });
// ReSharper restore CSharpWarnings::CS0612
		}

		public IEnumerable<IFieldable> SpatialGenerate(string fieldName, string shapeWKT,
			SpatialSearchStrategy spatialSearchStrategy = SpatialSearchStrategy.GeohashPrefixTree,
			int maxTreeLevel = 0, double distanceErrorPct = 0.025)
		{
			if (maxTreeLevel == 0) maxTreeLevel = GeohashPrefixTree.GetMaxLevelsPossible();
			var strategy = SpatialStrategies.GetOrAdd(fieldName, s => SpatialIndex.CreateStrategy(fieldName, spatialSearchStrategy, maxTreeLevel));

			var shape = SpatialIndex.ShapeReadWriter.ReadShape(shapeWKT);
			return strategy.CreateIndexableFields(shape)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, SpatialIndex.ShapeReadWriter.WriteShape(shape), Field.Store.YES, Field.Index.NO), });
		}

		#endregion
	}
}
