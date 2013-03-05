using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Spatial
{
	public abstract class AbstractSpatialShapeReader<T>
	{
		private static readonly Regex RegexX = new Regex("^(?:X|Longitude|Lng|Lon|Long)$", RegexOptions.IgnoreCase);
		private static readonly Regex RegexY = new Regex("^(?:Y|Latitude|Lat)$", RegexOptions.IgnoreCase);

		public abstract bool TryRead(object value, out T result);

		protected abstract T MakePoint(double x, double y);
		protected abstract T MakeCircle(double x, double y, double radius);

		public bool TryReadInner(object value, out T result)
		{
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

			result = default(T);
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
	}
}