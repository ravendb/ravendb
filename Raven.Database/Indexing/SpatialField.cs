//-----------------------------------------------------------------------
// <copyright file="SpatialField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using GeoAPI;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Spatial;
using Lucene.Net.Spatial.BBox;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Spatial.Util;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Spatial;
using Raven.Database.Indexing.Spatial;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Distance;
using Spatial4n.Core.Shapes;
using SpatialRelation = Raven.Abstractions.Indexing.SpatialRelation;

namespace Raven.Database.Indexing
{
	public class SpatialField
	{
		private static readonly NtsSpatialContext GeoContext;
		private static readonly ShapeConverter ShapeConverter;

		private readonly SpatialOptions options;
		private readonly NtsSpatialContext ntsContext;
		private readonly SpatialStrategy strategy;
		private readonly ShapeStringReadWriter shapeStringReadWriter;

		static SpatialField()
		{
			GeometryServiceProvider.Instance = new NtsGeometryServices();
			GeoContext = new NtsSpatialContext(true);
			ShapeConverter = new ShapeConverter();
		}

		public SpatialField(string fieldName, SpatialOptions options)
		{
			this.options = options;
			ntsContext = CreateNtsContext(options);
			shapeStringReadWriter = new ShapeStringReadWriter(options, ntsContext);
			strategy = CreateStrategy(fieldName, options, ntsContext);
		}

		private NtsSpatialContext CreateNtsContext(SpatialOptions opt)
		{
			if (opt.Type == SpatialFieldType.Cartesian)
			{
				var nts = new NtsSpatialContext(new GeometryFactory(), false, new CartesianDistCalc(), null);
				nts.GetWorldBounds().Reset(opt.MinX, opt.MaxX, opt.MinY, opt.MaxY);
				return nts;
			}
			return GeoContext;
		}

		public SpatialStrategy GetStrategy()
		{
			return strategy;
		}

		public NtsSpatialContext GetContext()
		{
			return ntsContext;
		}

		private SpatialStrategy CreateStrategy(string fieldName, SpatialOptions opt, NtsSpatialContext context)
		{
			switch (opt.Strategy)
			{
				case SpatialSearchStrategy.GeohashPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new GeohashPrefixTree(context, opt.MaxTreeLevel), fieldName);
				case SpatialSearchStrategy.QuadPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new QuadPrefixTree(context, opt.MaxTreeLevel), fieldName);
				case SpatialSearchStrategy.BoundingBox:
					return new BBoxStrategyThatSupportsAllShapes(context, fieldName);
			}
			return null;
		}

		public IEnumerable<AbstractField> CreateIndexableFields(object value)
		{
			var shape = value as Shape;
			if (shape != null || TryReadShape(value, out shape))
			{
				return strategy.CreateIndexableFields(shape)
					.Concat(new[] { new Field(Constants.SpatialShapeFieldName, WriteShape(shape), Field.Store.YES, Field.Index.NO), });
			}

			return Enumerable.Empty<AbstractField>();	
		}
		
		public Query MakeQuery(Query existingQuery, SpatialStrategy spatialStrategy, SpatialIndexQuery spatialQuery)
		{
			return MakeQuery(existingQuery, spatialStrategy, spatialQuery.QueryShape, spatialQuery.SpatialRelation, spatialQuery.DistanceErrorPercentage, spatialQuery.RadiusUnitOverride);
		}

		public Query MakeQuery(Query existingQuery, SpatialStrategy spatialStrategy, string shapeWKT, SpatialRelation relation,
									  double distanceErrorPct = 0.025, SpatialUnits? unitOverride = null)
		{
			SpatialOperation spatialOperation;
			var shape = ReadShape(shapeWKT, unitOverride);

			switch (relation)
			{
				case SpatialRelation.Within:
					spatialOperation = SpatialOperation.IsWithin;
					break;
				case SpatialRelation.Contains:
					spatialOperation = SpatialOperation.Contains;
					break;
				case SpatialRelation.Disjoint:
					spatialOperation = SpatialOperation.IsDisjointTo;
					break;
				case SpatialRelation.Intersects:
					spatialOperation = SpatialOperation.Intersects;
					break;
				case SpatialRelation.Nearby:
					// only sort by this, do not filter
					return new FunctionQuery(spatialStrategy.MakeDistanceValueSource(shape.GetCenter()));
				default:
					throw new ArgumentOutOfRangeException("relation");
			}
			var args = new SpatialArgs(spatialOperation, shape) { DistErrPct = distanceErrorPct };

			if (existingQuery is MatchAllDocsQuery)
				return new CustomScoreQuery(spatialStrategy.MakeQuery(args), new ValueSourceQuery(spatialStrategy.MakeRecipDistanceValueSource(shape)));
			return spatialStrategy.MakeQuery(args);
		}

		public Filter MakeFilter(SpatialStrategy spatialStrategy, IndexQuery indexQuery)
		{
			var spatialQry = indexQuery as SpatialIndexQuery;
			if (spatialQry == null) return null;

			var args = new SpatialArgs(SpatialOperation.IsWithin, ReadShape(spatialQry.QueryShape, spatialQry.RadiusUnitOverride));
			return spatialStrategy.MakeFilter(args);
		}

		public bool TryReadShape(object value, out Shape shape)
		{
			string shapeWkt;
			if (ShapeConverter.TryConvert(value, out shapeWkt))
			{
				shape = ReadShape(shapeWkt);
				return true;
			}
			shape = default(Shape);
			return false;
		}

		public Shape ReadShape(string shapeWKT, SpatialUnits? unitOverride = null)
		{
			return shapeStringReadWriter.ReadShape(shapeWKT, unitOverride);
		}

		public string WriteShape(Shape shape)
		{
			return shapeStringReadWriter.WriteShape(shape);
		}
	}
}