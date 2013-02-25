//-----------------------------------------------------------------------
// <copyright file="AbstractViewGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Text.RegularExpressions;
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
using Point = Spatial4n.Core.Shapes.Point;
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
		private readonly NtsShapeReadWriter shapeReadWriter;

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
			shapeReadWriter = GetShapeReadWriter(options, context);
		}

		public SpatialStrategy GetLuceneStrategy()
		{
			return strategy;
		}

		public NtsSpatialContext GetContext()
		{
			return context;
		}

		private NtsSpatialContext GetContext(SpatialOptions options)
		{
			if (options.Type == SpatialFieldType.Cartesian)
			{
				var nts = new NtsSpatialContext(new GeometryFactory(), false, new CartesianDistCalc(), null);
				nts.GetWorldBounds().Reset(options.MinX, options.MaxX, options.MinY, options.MaxY);
				return nts;
			}
			return GeoContext;
		}

		private NtsShapeReadWriter GetShapeReadWriter(SpatialOptions options, NtsSpatialContext ntsContext)
		{
			if (options.Type == SpatialFieldType.Cartesian)
				return new NtsShapeReadWriter(ntsContext);
			return GeoShapeReadWriter;
		}

		private SpatialStrategy CreateStrategy(string fieldName, SpatialOptions options)
		{
			switch (options.Strategy)
			{
				case SpatialSearchStrategy.GeohashPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new GeohashPrefixTree(context, options.MaxTreeLevel), fieldName);
				case SpatialSearchStrategy.QuadPrefixTree:
					return new RecursivePrefixTreeStrategyThatSupportsWithin(new QuadPrefixTree(context, options.MaxTreeLevel), fieldName);
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

		public Shape ReadShape(string shapeWKT)
		{
			if (options.Type == SpatialFieldType.Geography)
				shapeWKT = TranslateCircleFromKmToRadians(shapeWKT);
			Shape shape = shapeReadWriter.ReadShape(shapeWKT);
			return shape;
		}

		public string WriteShape(Shape shape)
		{
			return shapeReadWriter.WriteShape(shape);
		}

		private string TranslateCircleFromKmToRadians(string shapeWKT)
		{
			var match = CircleShape.Match(shapeWKT);
			if (match.Success == false)
				return shapeWKT;

			var radCapture = match.Groups[3];
			var radius = double.Parse(radCapture.Value, CultureInfo.InvariantCulture);

			radius = (radius / EarthMeanRadiusKm) * RadiansToDegrees;


			return shapeWKT.Substring(0, radCapture.Index) + radius.ToString("F6", CultureInfo.InvariantCulture) +
				   shapeWKT.Substring(radCapture.Index + radCapture.Length);

		}

		/// <summary>
		/// The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
		///
		/// [1] http://en.wikipedia.org/wiki/Earth_radius
		/// </summary>
		private const double EarthMeanRadiusKm = 6371.0087714;
		private const double DegreesToRadians = Math.PI / 180;
		private const double RadiansToDegrees = 1 / DegreesToRadians;

		private static readonly Regex CircleShape =
			new Regex(@"Circle \s* \( \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ d=([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* \)",
					  RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled | RegexOptions.IgnoreCase);

		// Shouldn't be here, a left only from SpatialIndex
		public static double GetGeographicalDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			var Context = new NtsSpatialContext(true);
			Point ptFrom = Context.MakePoint(fromLng, fromLat);
			Point ptTo = Context.MakePoint(toLng, toLat);
			var distance = Context.GetDistCalc().Distance(ptFrom, ptTo);
			return (distance / RadiansToDegrees) * EarthMeanRadiusKm;
		}
	}
}