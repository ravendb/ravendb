using System.Text.RegularExpressions;

namespace Raven.Abstractions.Spatial
{
	/// <summary>
	/// Santizes WKT strings, reducing them to 2D (discarding 3D and 4D values).
	/// We do this because we only index and query in 2D,
	/// but its nice to allow users to store shapes in 3D and 4D if they need to.
	/// </summary>
	public class WktSanitizer
	{
		private static readonly Regex RectangleRegex;
		private static readonly Regex DimensionFlagRegex;
		private static readonly Regex ReducerRegex;

		static WktSanitizer()
		{
			RegexOptions options;

#if SILVERLIGHT || NETFX_CORE
			options = RegexOptions.IgnorePatternWhitespace;
#else
			options = RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled;
#endif

			RectangleRegex = new Regex(@"^ \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* $", options);
			DimensionFlagRegex = new Regex(@"\s+ (?:z|m|zm|Z|M|ZM) \s* \(", options);
			ReducerRegex = new Regex(@"([+-]?(?:\d+\.?\d*|\d*\.?\d+) \s+ [+-]?(?:\d+\.?\d*|\d*\.?\d+)) (?:\s+[+-]?(?:\d+\.?\d*|\d*\.?\d+))+", options);
		}

		public string Sanitize(string shapeWkt)
		{
			if (RectangleRegex.IsMatch(shapeWkt))
				return shapeWkt;

			shapeWkt = DimensionFlagRegex.Replace(shapeWkt, " (");

			return ReducerRegex.Replace(shapeWkt, "$1");
		}
	}
}