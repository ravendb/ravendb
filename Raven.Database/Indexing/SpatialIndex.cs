//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using GeoAPI;
using Lucene.Net.Search;
using Lucene.Net.Spatial;
using Lucene.Net.Spatial.Prefix;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Spatial.Util;
using NetTopologySuite;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Spatial4n.Core.Context;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Shapes;
using SpatialRelation = Spatial4n.Core.Shapes.SpatialRelation;

namespace Raven.Database.Indexing
{
	public static class SpatialIndex
	{
		internal static readonly SpatialContext Context;

		static SpatialIndex()
		{
			Context = NtsSpatialContext.GEO_KM;
			GeometryServiceProvider.Instance = new NtsGeometryServices();
		}

		public static SpatialStrategy CreateStrategy(string fieldName, SpatialSearchStrategy spatialSearchStrategy, int maxTreeLevel)
		{
			switch (spatialSearchStrategy)
			{
				case SpatialSearchStrategy.GeohashPrefixTree:
					return new RecursivePrefixTreeStrategy(new GeohashPrefixTree(Context, maxTreeLevel), fieldName);
				case SpatialSearchStrategy.QuadPrefixTree:
					return new RecursivePrefixTreeStrategy(new QuadPrefixTree(Context, maxTreeLevel), fieldName);
			}
			return null;
		}

		public static Query MakeQuery(SpatialStrategy spatialStrategy, string shapeWKT, Abstractions.Indexing.SpatialRelation relation, double distanceErrorPct = 0.025)
		{
			SpatialOperation spatialOperation;
			var shape = Context.ReadShape(shapeWKT);
			switch (relation)
			{
				case Abstractions.Indexing.SpatialRelation.Within:
					spatialOperation = SpatialOperation.IsWithin;
					break;
				case Abstractions.Indexing.SpatialRelation.Contains:
					spatialOperation = SpatialOperation.Contains;
					break;
				case Abstractions.Indexing.SpatialRelation.Disjoint:
					spatialOperation = SpatialOperation.IsDisjointTo;
					break;
				case Abstractions.Indexing.SpatialRelation.Intersects:
					spatialOperation = SpatialOperation.Intersects;
					break;
				case Abstractions.Indexing.SpatialRelation.Nearby:
					var nearbyArgs = new SpatialArgs(SpatialOperation.IsWithin, shape);
					nearbyArgs.SetDistPrecision(distanceErrorPct);
					// only sort by this, do not filter
					return new FunctionQuery(spatialStrategy.MakeValueSource(nearbyArgs));
				default:
					throw new ArgumentOutOfRangeException("relation");
			}
			var args = new SpatialArgs(spatialOperation, shape);
			args.SetDistPrecision(distanceErrorPct);

			return spatialStrategy.MakeQuery(args);
		}

		public static Filter MakeFilter(SpatialStrategy spatialStrategy, IndexQuery indexQuery)
		{
			var spatialQry = indexQuery as SpatialIndexQuery;
			if (spatialQry == null) return null;

			var args = new SpatialArgs(SpatialOperation.IsWithin, Context.ReadShape(spatialQry.QueryShape));
			return spatialStrategy.MakeFilter(args);
		}

		public static double GetDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			var ptFrom = Context.MakePoint(fromLng, fromLat);
			var ptTo = Context.MakePoint(toLng, toLat);
			return Context.GetDistCalc().Distance(ptFrom, ptTo);
		}
	}
}