//-----------------------------------------------------------------------
// <copyright file="RavenShapeReadWriter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Spatial;

namespace Raven.Database.Indexing.Spatial
{
	public class RavenShapeConverter : ShapeConverter
	{
		private readonly SpatialOptions options;

		private static readonly Regex RegexGeoUriCoord = new Regex(@"([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* , \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* ,? \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+))?",
				RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);
		private static readonly Regex RegexGeoUriUncert = new Regex(@"u \s* = \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+))",
						RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase);

		public RavenShapeConverter(SpatialOptions options)
		{
			this.options = options;
		}

		public override bool TryConvert(object value, out string result)
		{
			if (value == null)
			{
				result = default(string);
				return false;
			}

			if (base.TryConvert(value, out result))
				return true;

			var str = value as string;
			if (!string.IsNullOrWhiteSpace(str))
			{
				if (TryParseGeoUri(str, out result))
					return true;

				result = str;
				return true;
			}

			result = default(string);
			return false;
		}

		private bool TryParseGeoUri(string uriString, out string shape)
		{
			shape = default(string);

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
					if (uncertainty > 0)
						uncertainty = uncertainty / 1000; // meters
				}
			}

			if (coordinate == null)
				return false;

			if (!double.IsNaN(uncertainty) && uncertainty > 0)
				shape = MakeCircle(coordinate[0], coordinate[1], uncertainty);
			else
				shape = MakePoint(coordinate[0], coordinate[1]);

			return true;
		}
	}
}
