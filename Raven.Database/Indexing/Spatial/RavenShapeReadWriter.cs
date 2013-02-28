//-----------------------------------------------------------------------
// <copyright file="RavenShapeReadWriter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Indexing.Spatial.GeoJson;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Spatial
{
	public class RavenShapeReadWriter
	{
		private readonly NtsSpatialContext context;
		private readonly SpatialOptions options;
		private readonly NtsShapeReadWriter shapeReadWriter;

		private static readonly Regex RegexX = new Regex("^(?:X|Longitude|Lng|Lon|Long)$", RegexOptions.IgnoreCase);
		private static readonly Regex RegexY = new Regex("^(?:Y|Latitude|Lat)$", RegexOptions.IgnoreCase);
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

		public bool TryReadShape(object value, out Shape shape)
		{
			shape = null;

			if (value == null)
				return false;

			var enumerable = value as IEnumerable;
			if (enumerable != null)
			{
				var list = enumerable.Cast<object>().ToList();
				if (list.Count > 1 && list.All(IsNumber))
				{
					shape = context.MakePoint(GetDouble(list[0]), GetDouble(list[1]));
					return true;
				}

				var keyValues = list.OfType<KeyValuePair<string, object>>()
					.Where(x => IsNumber(x.Value))
					.ToDictionary(x => x.Key, x => x.Value);

				if (keyValues.Count > 1)
				{
					var x1 = keyValues.Select(x => x.Key).FirstOrDefault(c => RegexX.IsMatch(c));
					var y1 = keyValues.Select(x => x.Key).FirstOrDefault(c => RegexY.IsMatch(c));

					if (x1 != null && y1 != null)
					{
						shape = context.MakePoint(GetDouble(keyValues[x1]), GetDouble(keyValues[y1]));
						return true;
					}
				}
			}

			var jsonObject = value as IDynamicJsonObject;
			if (jsonObject != null)
			{
				var geoJson = new GeoJsonShapeConverter(context);
				return geoJson.TryConvert(jsonObject.Inner, out shape);
			}

			var str = value as string;
			if (!string.IsNullOrWhiteSpace(str))
			{
				if (TryParseGeoUri(str, out shape))
					return true;

				if (options.Type == SpatialFieldType.Geography)
					str = TranslateCircleFromKmToRadians(str);
				shape = shapeReadWriter.ReadShape(str);
				return true;
			}
			return false;
		}

		public bool TryParseGeoUri(string uriString, out Shape shape)
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

		private bool IsNumber(object obj)
		{
			var rValue = obj as RavenJValue;
			return obj is double
			       || obj is float
			       || obj is int
			       || obj is long
			       || obj is short
				   || rValue != null && (rValue.Type == JTokenType.Float || rValue.Type == JTokenType.Integer);
		}

		private double GetDouble(object obj)
		{
			if (obj is double || obj is float || obj is int || obj is long || obj is short)
				return Convert.ToDouble(obj);

			var rValue = obj as RavenJValue;
			if (rValue != null && (rValue.Type == JTokenType.Float || rValue.Type == JTokenType.Integer))
				return Convert.ToDouble(rValue.Value);

			return 0d;
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
