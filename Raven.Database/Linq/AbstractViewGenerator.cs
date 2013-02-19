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
using Raven.Abstractions.Linq;
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
				if (countOfSelectMany == null)
				{
					countOfSelectMany = selectManyOrFrom.Matches(ViewText).Count;
				}
				return countOfSelectMany.Value;
			}
		}

		public int CountOfFields { get { return fields.Count; } }

		public List<IndexingFunc> MapDefinitions { get; private set; }

		public IndexingFunc ReduceDefinition { get; set; }

		public TranslatorFunc TransformResultsDefinition { get; set; }

		public GroupByKeyFunc GroupByExtraction { get; set; }

		public string ViewText { get; set; }

		public IDictionary<string, FieldStorage> Stores { get; set; }

		public IDictionary<string, FieldIndexing> Indexes { get; set; }

		public IDictionary<string, FieldTermVector> TermVectors { get; set; } 

		public HashSet<string> ForEntityNames { get; set; }

		public string[] Fields
		{
			get { return fields.ToArray(); }
		}

		public bool HasWhereClause
		{
			get
			{
				if (hasWhereClause == null)
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
			TermVectors = new Dictionary<string, FieldTermVector>();
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

		protected dynamic LoadDocument(string key)
		{
			if (CurrentIndexingScope.Current == null)
				throw new InvalidOperationException("LoadDocument may only be called from the map portion of the index. Was called with: " + key);

			return CurrentIndexingScope.Current.LoadDocument(key);
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

		private ConcurrentDictionary<string, SpatialStrategy> SpatialStrategies { get; set; }

		public IEnumerable<IFieldable> SpatialGenerate(double? lat, double? lng)
		{
			return SpatialGenerate(Constants.DefaultSpatialFieldName, lat, lng);
		}

		public IEnumerable<IFieldable> SpatialGenerate(string fieldName, double? lat, double? lng)
		{
			var strategy = GetStrategyForField(fieldName);

			if (lng == null || double.IsNaN(lng.Value))
				return Enumerable.Empty<IFieldable>();
			if(lat == null || double.IsNaN(lat.Value))
				return Enumerable.Empty<IFieldable>();

			Shape shape = SpatialIndex.Context.MakePoint(lng.Value, lat.Value);
			return strategy.CreateIndexableFields(shape)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, SpatialIndex.WriteShape(shape), Field.Store.YES, Field.Index.NO), });
		}

		[CLSCompliant(false)]
		public SpatialStrategy GetStrategyForField(string fieldName)
		{
			return SpatialStrategies.GetOrAdd(fieldName, s =>
			{
				if (SpatialStrategies.Count > 1024)
				{
					throw new InvalidOperationException("The number of spatial fields in an index is limited ot 1,024");
				}
				return SpatialIndex.CreateStrategy(fieldName, SpatialSearchStrategy.GeohashPrefixTree, GeohashPrefixTree.GetMaxLevelsPossible());
			});
		}

		public IEnumerable<IFieldable> SpatialGenerate(string fieldName, string shapeWKT,
			SpatialSearchStrategy spatialSearchStrategy = SpatialSearchStrategy.GeohashPrefixTree,
			int maxTreeLevel = 0, double distanceErrorPct = 0.025)
		{
			if (string.IsNullOrEmpty(shapeWKT))
				return Enumerable.Empty<IFieldable>();

			if (maxTreeLevel == 0)
			{
				switch (spatialSearchStrategy)
				{
					case SpatialSearchStrategy.GeohashPrefixTree:
						maxTreeLevel = 9; // about 2 meters, should be good enough (see: http://unterbahn.com/2009/11/metric-dimensions-of-geohash-partitions-at-the-equator/)
						break;
					case SpatialSearchStrategy.QuadPrefixTree:
						maxTreeLevel = 25; // about 1 meter, should be good enough
						break;
					default:
						throw new ArgumentOutOfRangeException("spatialSearchStrategy");
				}
			}
			var strategy = SpatialStrategies.GetOrAdd(fieldName, s => SpatialIndex.CreateStrategy(fieldName, spatialSearchStrategy, maxTreeLevel));

			var shape = SpatialIndex.ReadShape(shapeWKT);
			return strategy.CreateIndexableFields(shape)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, SpatialIndex.WriteShape(shape), Field.Store.YES, Field.Index.NO), });
		}

		#endregion
	}
}
