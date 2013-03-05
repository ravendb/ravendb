//-----------------------------------------------------------------------
// <copyright file="RavenShapeReadWriter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Spatial;
using Raven.Database.Indexing.Spatial.GeoJson;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Spatial
{
	public class RavenShapeReadWriter : AbstractSpatialShapeReader<Shape>
	{
		private readonly NtsSpatialContext context;
		private readonly SpatialOptions options;
		private readonly NtsShapeReadWriter shapeReadWriter;

		private static readonly Regex RegexGeoUriCoord = new Regex(@"([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* , \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* ,? \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+))?",
				RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);
		private static readonly Regex RegexGeoUriUncert = new Regex(@"u \s* = \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+))",
						RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);

		public RavenShapeReadWriter(NtsSpatialContext context, SpatialOptions options, NtsShapeReadWriter shapeReadWriter)
		{
			this.context = context;
			this.options = options;
			this.shapeReadWriter = shapeReadWriter;
		}

		public override bool TryRead(object value, out Shape result)
		{
			if (value == null)
			{
				result = default(Shape);
				return false;
			}

			if (TryReadInner(value, out result))
				return true;

			var jsonObject = value as IDynamicJsonObject;
			if (jsonObject != null)
			{
				var geoJson = new GeoJsonShapeConverter(context);
				return geoJson.TryConvert(jsonObject.Inner, out result);
			}

			var str = value as string;
			if (!string.IsNullOrWhiteSpace(str))
			{
				if (TryParseGeoUri(str, out result))
					return true;

				if (options.Type == SpatialFieldType.Geography)
					str = TranslateCircleFromKmToRadians(str);
				result = shapeReadWriter.ReadShape(str);
				return true;
			}

			result = default(Shape);
			return false;
		}

		protected override Shape MakePoint(double x, double y)
		{
			return context.MakePoint(x, y);
		}

		protected override Shape MakeCircle(double x, double y, double radius)
		{
			return context.MakeCircle(x, y, radius);
		}

		private bool TryParseGeoUri(string uriString, out Shape shape)
		{
			shape = default(Shape);

			// Geo URI should be used for geographic locations,
			// so for the time-being we'll only support if for geography indexes
			if (options.Type != SpatialFieldType.Geography)
				return false;

			if (string.IsNullOrWhiteSpace(uriString))
				return false;

			uriString = uriString.Trim();

			if (!uriString.StartsWith("geo:"))
				return false;

			var components = uriString.Substring(4, uriString.Length - 4).Split(';').Select(x => x.Trim());

			double[] coordinate = null;
			var uncertainty = double.NaN;

			foreach (var component in components)
			{
				var coord = RegexGeoUriCoord.Match(component);
				if (coord.Success)
				{
					if (coord.Groups.Count > 1)
						coordinate = new [] {double.Parse(coord.Groups[1].Value), double.Parse(coord.Groups[2].Value)};
					continue;
				}
				var u = RegexGeoUriUncert.Match(component);
				if (u.Success)
				{
					uncertainty = double.Parse(u.Groups[1].Value);
					if (options.Type == SpatialFieldType.Geography && uncertainty > 0)
						uncertainty = TranslateCircleFromKmToRadians(uncertainty / 1000); // meters
				}
			}

			if (coordinate == null)
				return false;

			if (!double.IsNaN(uncertainty) && uncertainty > 0)
				shape = context.MakeCircle(coordinate[0], coordinate[1], uncertainty);
			else
				shape = context.MakePoint(coordinate[0], coordinate[1]);

			return true;
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

		private double TranslateCircleFromKmToRadians(double radius)
		{
			return (radius / EarthMeanRadiusKm) * RadiansToDegrees;
		}

		private string TranslateCircleFromKmToRadians(string shapeWKT)
		{
			var match = CircleShape.Match(shapeWKT);
			if (match.Success == false)
				return shapeWKT;

			var radCapture = match.Groups[3];
			var radius = double.Parse(radCapture.Value, CultureInfo.InvariantCulture);

			radius = TranslateCircleFromKmToRadians(radius);

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
	}
}
