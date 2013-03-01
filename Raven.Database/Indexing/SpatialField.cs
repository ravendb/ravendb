//-----------------------------------------------------------------------
// <copyright file="SpatialField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using GeoAPI;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Spatial;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Spatial.Util;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing.Spatial;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Distance;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;
using SpatialRelation = Raven.Abstractions.Indexing.SpatialRelation;

namespace Raven.Database.Indexing
{
	public class SpatialField
	{
		private static readonly NtsSpatialContext GeoContext;
		private static readonly NtsShapeReadWriter GeoShapeReadWriter;

		private readonly SpatialOptions options;
		private readonly NtsSpatialContext context;
		private readonly SpatialStrategy strategy;
		private readonly RavenShapeReadWriter shapeReadWriter;

		static SpatialField()
		{
			GeometryServiceProvider.Instance = new NtsGeometryServices();
			GeoContext = new NtsSpatialContext(true);
			GeoShapeReadWriter = new NtsShapeReadWriter(GeoContext);
		}

		public SpatialField(string fieldName, SpatialOptions options)
		{
			this.options = options;
			context = GetContext(options);
			strategy = CreateStrategy(fieldName, options);
			shapeReadWriter = new RavenShapeReadWriter(context, options, GetShapeReadWriter(options, context));
		}

		public SpatialStrategy GetStrategy()
		{
			return strategy;
		}

		public NtsSpatialContext GetContext()
		{
			return context;
		}

		private NtsSpatialContext GetContext(SpatialOptions opt)
		{
			if (opt.Type == SpatialFieldType.Cartesian)
			{
				var nts = new NtsSpatialContext(new GeometryFactory(), false, new CartesianDistCalc(), null);
				nts.GetWorldBounds().Reset(opt.MinX, opt.MaxX, opt.MinY, opt.MaxY);
				return nts;
			}
			return GeoContext;
		}

		private NtsShapeReadWriter GetShapeReadWriter(SpatialOptions opt, NtsSpatialContext ntsContext)
		{
			if (opt.Type == SpatialFieldType.Cartesian)
				return new NtsShapeReadWriter(ntsContext);
			return GeoShapeReadWriter;
		}

		private SpatialStrategy CreateStrategy(string fieldName, SpatialOptions opt)
		{
			switch (opt.Strategy)
			{
				case SpatialSearchStrategy.GeohashPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new GeohashPrefixTree(context, opt.MaxTreeLevel), fieldName);
				case SpatialSearchStrategy.QuadPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new QuadPrefixTree(context, opt.MaxTreeLevel), fieldName);
			}
			return null;
		}

		public Query MakeQuery(Query existingQuery, SpatialStrategy spatialStrategy, string shapeWKT, SpatialRelation relation,
									  double distanceErrorPct = 0.025)
		{
			SpatialOperation spatialOperation;
			var shape = ReadShape(shapeWKT);

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

			var args = new SpatialArgs(SpatialOperation.IsWithin, shapeReadWriter.ReadShape(spatialQry.QueryShape));
			return spatialStrategy.MakeFilter(args);
		}

		public bool TryReadShape(object value, out Shape shape)
		{
			return shapeReadWriter.TryReadShape(value, out shape);
		}

		public Shape ReadShape(string shapeWKT)
		{
			return shapeReadWriter.ReadShape(shapeWKT);
		}

		public string WriteShape(Shape shape)
		{
			return shapeReadWriter.WriteShape(shape);
		}
	}
}