using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Spatial;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;

namespace Raven.Database.Indexing.Spatial
{
	/// <summary>
	/// Reads and writes shape strings
	/// </summary>
	public class ShapeStringReadWriter
	{
		private static readonly WktSanitizer WktSanitizer = new WktSanitizer();
		private static NtsShapeReadWriter geoShapeReadWriter;

		private readonly SpatialOptions options;
		private readonly NtsShapeReadWriter ntsShapeReadWriter;
		private readonly ShapeStringConverter shapeStringConverter;

		public ShapeStringReadWriter(SpatialOptions options, NtsSpatialContext context)
		{
			this.options = options;
			this.ntsShapeReadWriter = CreateNtsShapeReadWriter(options, context);
			this.shapeStringConverter = new ShapeStringConverter(options);
		}

		private NtsShapeReadWriter CreateNtsShapeReadWriter(SpatialOptions opt, NtsSpatialContext ntsContext)
		{
			if (opt.Type == SpatialFieldType.Cartesian)
				return new NtsShapeReadWriter(ntsContext);
			return geoShapeReadWriter ?? (geoShapeReadWriter = new NtsShapeReadWriter(ntsContext));
		}

		public Shape ReadShape(string shape)
		{
			shape = shapeStringConverter.ConvertToWKT(shape);
			shape = WktSanitizer.Sanitize(shape);

			// Circle translation should be done last, before passing to NtsShapeReadWriter
			if (options.Type == SpatialFieldType.Geography)
				shape = TranslateCircleFromKmToRadians(shape);

			return ntsShapeReadWriter.ReadShape(shape);
		}

		public string WriteShape(Shape shape)
		{
			return ntsShapeReadWriter.WriteShape(shape);
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
