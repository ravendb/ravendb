using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
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
				return new NtsShapeReadWriter(ntsContext, false);
			return geoShapeReadWriter ?? (geoShapeReadWriter = new NtsShapeReadWriter(ntsContext, false));
		}

		public Shape ReadShape(string shape, SpatialUnits? unitOverride = null)
		{
			shape = shapeStringConverter.ConvertToWKT(shape);
			shape = WktSanitizer.Sanitize(shape);

			// Circle translation should be done last, before passing to NtsShapeReadWriter
			if (options.Type == SpatialFieldType.Geography)
				shape = TranslateCircleRadius(shape, unitOverride.HasValue ? unitOverride.Value : options.Units);

			return ntsShapeReadWriter.ReadShape(shape);
		}

		public string WriteShape(Shape shape)
		{
			return ntsShapeReadWriter.WriteShape(shape);
		}

		private double TranslateCircleRadius(double radius, SpatialUnits units)
		{
			if (units == SpatialUnits.Miles)
				radius *= Constants.MilesToKm;

			return (radius / Constants.EarthMeanRadiusKm) * RadiansToDegrees;
		}

		private string TranslateCircleRadius(string shapeWKT, SpatialUnits units)
		{
			var match = CircleShape.Match(shapeWKT);
			if (match.Success == false)
				return shapeWKT;

			var radCapture = match.Groups[3];
			var radius = double.Parse(radCapture.Value, CultureInfo.InvariantCulture);

			radius = TranslateCircleRadius(radius, units);

			return shapeWKT.Substring(0, radCapture.Index) + radius.ToString("F6", CultureInfo.InvariantCulture) +
				   shapeWKT.Substring(radCapture.Index + radCapture.Length);
		}

		private const double DegreesToRadians = Math.PI / 180;
		private const double RadiansToDegrees = 1 / DegreesToRadians;

		private static readonly Regex CircleShape =
			new Regex(@"Circle \s* \( \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ d=([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* \)",
					  RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled | RegexOptions.IgnoreCase);
	}
}
