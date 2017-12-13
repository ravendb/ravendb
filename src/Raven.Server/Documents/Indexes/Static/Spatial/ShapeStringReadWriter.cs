using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Spatial;
using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Io;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    /// <summary>
    /// Reads and writes shape strings
    /// </summary>
    public class ShapeStringReadWriter
    {
        private static readonly WktSanitizer WktSanitizer = new WktSanitizer();
        private static NtsShapeReadWriter _geoShapeReadWriter;

        private readonly SpatialOptions _options;
        private readonly NtsShapeReadWriter _ntsShapeReadWriter;
        private readonly ShapeStringConverter _shapeStringConverter;

        public ShapeStringReadWriter(SpatialOptions options, NtsSpatialContext context)
        {
            _options = options;
            _ntsShapeReadWriter = CreateNtsShapeReadWriter(options, context);
            _shapeStringConverter = new ShapeStringConverter(options);
        }

        private static NtsShapeReadWriter CreateNtsShapeReadWriter(SpatialOptions opt, NtsSpatialContext ntsContext)
        {
            if (opt.Type == SpatialFieldType.Cartesian)
                return new NtsShapeReadWriter(ntsContext, false);
            return _geoShapeReadWriter ?? (_geoShapeReadWriter = new NtsShapeReadWriter(ntsContext, false));
        }

        public Shape ReadShape(string shape, SpatialUnits? unitOverride = null)
        {
            shape = _shapeStringConverter.ConvertToWKT(shape);
            shape = WktSanitizer.Sanitize(shape);

            // Circle translation should be done last, before passing to NtsShapeReadWriter
            if (_options.Type == SpatialFieldType.Geography)
                shape = TranslateCircleRadius(shape, unitOverride ?? _options.Units);

            return _ntsShapeReadWriter.ReadShape(shape);
        }

        public string WriteShape(Shape shape)
        {
            return _ntsShapeReadWriter.WriteShape(shape);
        }

        public static double TranslateCircleRadius(double radius, SpatialUnits units)
        {
            if (units == SpatialUnits.Miles)
                radius *= Constants.Documents.Indexing.Spatial.MilesToKm;

            return (radius / Constants.Documents.Indexing.Spatial.EarthMeanRadiusKm) * RadiansToDegrees;
        }

        private static string TranslateCircleRadius(string shapeWkt, SpatialUnits units)
        {
            var match = CircleShape.Match(shapeWkt);
            if (match.Success == false)
                return shapeWkt;

            var radCapture = match.Groups[3];
            var radius = double.Parse(radCapture.Value, CultureInfo.InvariantCulture);

            radius = TranslateCircleRadius(radius, units);

            return shapeWkt.Substring(0, radCapture.Index) + radius.ToString("F6", CultureInfo.InvariantCulture) +
                   shapeWkt.Substring(radCapture.Index + radCapture.Length);
        }

        private const double DegreesToRadians = Math.PI / 180;
        private const double RadiansToDegrees = 1 / DegreesToRadians;

        private static readonly Regex CircleShape =
            new Regex(@"Circle \s* \( \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ d=([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* \)",
                      RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled | RegexOptions.IgnoreCase);
    }
}
