//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Text.RegularExpressions;
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
using Raven.Database.Indexing.Spatial;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Distance;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Impl;
using Point = Spatial4n.Core.Shapes.Point;
using SpatialRelation = Raven.Abstractions.Indexing.SpatialRelation;

namespace Raven.Database.Indexing
{
	[CLSCompliant(false)]
	public static class SpatialIndex
	{
		internal static readonly NtsSpatialContext Context;
		private static readonly NtsShapeReadWriter shapeReadWriter;
		/// <summary>
		/// The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
		///
		/// [1] http://en.wikipedia.org/wiki/Earth_radius
		/// </summary>
		internal const double EarthMeanRadiusKm = 6371.0087714;
		internal const double DegreesToRadians = Math.PI / 180;
		internal const double RadiansToDegrees = 1 / DegreesToRadians;

		static SpatialIndex()
		{
			Context = new NtsSpatialContext(true);
			GeometryServiceProvider.Instance = new NtsGeometryServices();

			shapeReadWriter = new NtsShapeReadWriter(Context);
		}

		public static SpatialStrategy CreateStrategy(string fieldName, SpatialSearchStrategy spatialSearchStrategy,
													 int maxTreeLevel)
		{
			switch (spatialSearchStrategy)
			{
				case SpatialSearchStrategy.GeohashPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new GeohashPrefixTree(Context, maxTreeLevel), fieldName);
				case SpatialSearchStrategy.QuadPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new QuadPrefixTree(Context, maxTreeLevel), fieldName);
			}
			return null;
		}

		public static Query MakeQuery(SpatialStrategy spatialStrategy, string shapeWKT, SpatialRelation relation,
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

			return spatialStrategy.MakeQuery(args);
		}

		public static Shape ReadShape(string shapeWKT)
		{
			shapeWKT = TranslateCircleFromKmToRadians(shapeWKT);
			Shape shape = shapeReadWriter.ReadShape(shapeWKT);
			return shape;
		}

		private static readonly Regex CirlceShape =
			new Regex(@"Circle \s* \( \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ d=([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* \)",
					  RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled | RegexOptions.IgnoreCase);
		
		private static string TranslateCircleFromKmToRadians(string shapeWKT)
		{
			var match = CirlceShape.Match(shapeWKT);
			if(match.Success == false)
				return shapeWKT;

			var radCapture = match.Groups[3];
			var radius = double.Parse(radCapture.Value);

			radius = (radius / EarthMeanRadiusKm) * RadiansToDegrees;


			return shapeWKT.Substring(0, radCapture.Index) + radius.ToString("F6", CultureInfo.InvariantCulture) +
			       shapeWKT.Substring(radCapture.Index + radCapture.Length);

		}

		public static Filter MakeFilter(SpatialStrategy spatialStrategy, IndexQuery indexQuery)
		{
			var spatialQry = indexQuery as SpatialIndexQuery;
			if (spatialQry == null) return null;

			var args = new SpatialArgs(SpatialOperation.IsWithin, shapeReadWriter.ReadShape(spatialQry.QueryShape));
			return spatialStrategy.MakeFilter(args);
		}

		public static double GetDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			Point ptFrom = Context.MakePoint(fromLng, fromLat);
			Point ptTo = Context.MakePoint(toLng, toLat);
			var distance = Context.GetDistCalc().Distance(ptFrom, ptTo);
			return (distance / RadiansToDegrees) * EarthMeanRadiusKm;
		}

		public static string WriteShape(Shape shape)
		{
			var writeShape = shapeReadWriter.WriteShape(shape);
			return writeShape;
		}
	}
}