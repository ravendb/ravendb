using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    /// <summary>
    /// Converts spatial strings to WKT, if they aren't already
    /// </summary>
    public class ShapeStringConverter
    {
        private readonly SpatialOptions _options;

        private static readonly Regex RegexBox = new Regex(@"^BOX(?:2D)? \s* \( \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* , \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s+ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* \)$",
                                                                   RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static readonly Regex RegexGeoUriCoord = new Regex(@"^ ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* , \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) \s* ,? \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+))? $",
                                                                   RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase | RegexOptions.Compiled);
        private static readonly Regex RegexGeoUriUncert = new Regex(@"^ u \s* = \s* ([+-]?(?:\d+\.?\d*|\d*\.?\d+)) $",
                                                                    RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public ShapeStringConverter(SpatialOptions options)
        {
            _options = options;
        }

        public string ConvertToWKT(string shape)
        {
            if (!string.IsNullOrWhiteSpace(shape))
            {
                shape = shape.Trim();
                string result;
                if (TryParseGeoUri(shape, out result))
                    return result;
                if (TryParseBox(shape, out result))
                    return result;

                return shape;
            }

            return default(string);
        }

        private static bool TryParseBox(string value, out string shape)
        {
            shape = default(string);

            if (!value.StartsWith("BOX", StringComparison.OrdinalIgnoreCase))
                return false;
            var match = RegexBox.Match(value);
            if (match.Success)
            {
                shape = $"{match.Groups[1].Value} {match.Groups[2].Value} {match.Groups[3].Value} {match.Groups[4].Value}";
                return true;
            }

            return false;
        }

        private bool TryParseGeoUri(string uriString, out string shape)
        {
            shape = default(string);

            if (!uriString.StartsWith("geo:", StringComparison.OrdinalIgnoreCase))
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
                        coordinate = new[] { double.Parse(coord.Groups[1].Value, CultureInfo.InvariantCulture), double.Parse(coord.Groups[2].Value, CultureInfo.InvariantCulture) };
                    continue;
                }
                var u = RegexGeoUriUncert.Match(component);
                if (u.Success)
                {
                    uncertainty = double.Parse(u.Groups[1].Value, CultureInfo.InvariantCulture);

                    // Uncertainty is in meters when in a geographic context
                    if (uncertainty > 0 && _options.Type == SpatialFieldType.Geography)
                    {
                        uncertainty = uncertainty / 1000;
                        if (_options.Units == SpatialUnits.Miles)
                            uncertainty *= KmToMiles;
                    }
                }
            }

            if (coordinate == null)
                return false;

            if (_options.Type == SpatialFieldType.Geography)
                coordinate = new[] { coordinate[1], coordinate[0] };

            if (!double.IsNaN(uncertainty) && uncertainty > 0)
                shape = MakeCircle(coordinate[0], coordinate[1], uncertainty);
            else
                shape = MakePoint(coordinate[0], coordinate[1]);

            return true;
        }

        protected string MakePoint(double x, double y)
        {
            return string.Format(CultureInfo.InvariantCulture, "POINT ({0} {1})", x, y);
        }

        protected string MakeCircle(double x, double y, double radius)
        {
            return string.Format(CultureInfo.InvariantCulture, "Circle({0} {1} d={2})", x, y, radius);
        }

        private const double KmToMiles = 0.621371;
    }
}
