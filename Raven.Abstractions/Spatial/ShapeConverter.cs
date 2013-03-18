using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Spatial
{
	/// <summary>
	/// Converts shape objects to strings, if they are not already a string
	/// </summary>
	public class ShapeConverter
	{
		private static readonly GeoJsonWktConverter GeoJsonConverter = new GeoJsonWktConverter();
		private static readonly Regex RegexX = new Regex("^(?:X|Longitude|Lng|Lon|Long)$", RegexOptions.IgnoreCase);
		private static readonly Regex RegexY = new Regex("^(?:Y|Latitude|Lat)$", RegexOptions.IgnoreCase);

		public virtual bool TryConvert(object value, out string result)
		{
			var s = value as string;
			if (s != null)
			{
				result = s;
				return true;
			}

			var jValue = value as RavenJValue;
			if (jValue != null && jValue.Type == JTokenType.String)
			{
				result = (string)jValue.Value;
				return true;
			}

			var enumerable = value as IEnumerable;
			if (enumerable != null)
			{
				var list = enumerable.Cast<object>().ToList();
				if (list.Count > 1 && list.All(IsNumber))
				{
					result = MakePoint(GetDouble(list[0]), GetDouble(list[1]));
					return true;
				}

				var keyValues = list.OfType<KeyValuePair<string, object>>()
				                    .Where(x => IsNumber(x.Value))
				                    .ToDictionary(x => x.Key, x => x.Value);

				if (keyValues.Count == 0)
				{
					keyValues = list.OfType<KeyValuePair<string, RavenJToken>>()
									.Where(x => IsNumber(x.Value))
									.ToDictionary(x => x.Key, x => (object) x.Value);
				}

				if (keyValues.Count > 1)
				{
					var x1 = keyValues.Select(x => x.Key).FirstOrDefault(c => RegexX.IsMatch(c));
					var y1 = keyValues.Select(x => x.Key).FirstOrDefault(c => RegexY.IsMatch(c));

					if (x1 != null && y1 != null)
					{
						result = MakePoint(GetDouble(keyValues[x1]), GetDouble(keyValues[y1]));
						return true;
					}
				}
			}

			var djObj = value as IDynamicJsonObject;
			var jObj = djObj != null ? djObj.Inner : value as RavenJObject;

			if (jObj != null && GeoJsonConverter.TryConvert(jObj, out result))
				return true;

			result = default(string);
			return false;
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

		protected string MakePoint(double x, double y)
		{
			return string.Format(CultureInfo.InvariantCulture, "POINT ({0} {1})", x, y);
		}

		protected string MakeCircle(double x, double y, double radius)
		{
			return string.Format(CultureInfo.InvariantCulture, "Circle({0} {1} d={2})", x, y, radius);
		}
	}
}