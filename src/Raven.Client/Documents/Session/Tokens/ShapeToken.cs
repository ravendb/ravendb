using System.Text;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Session.Tokens
{
    public class ShapeToken : QueryToken
    {
        private readonly string _shape;

        private ShapeToken(string shape)
        {
            _shape = shape;
        }

        public static ShapeToken Circle(string radiusParameterName, string latituteParameterName, string longitudeParameterName, SpatialUnits? radiusUnits)
        {
            if (radiusUnits.HasValue == false)
                return new ShapeToken($"spatial.circle(${radiusParameterName}, ${latituteParameterName}, ${longitudeParameterName})");

            if (radiusUnits == SpatialUnits.Kilometers)
                return new ShapeToken($"spatial.circle(${radiusParameterName}, ${latituteParameterName}, ${longitudeParameterName}, '{nameof(SpatialUnits.Kilometers)}')");

            return new ShapeToken($"spatial.circle(${radiusParameterName}, ${latituteParameterName}, ${longitudeParameterName}, '{nameof(SpatialUnits.Miles)}')");
        }

        public static ShapeToken Wkt(string shapeWktParameterName)
        {
            return new ShapeToken($"spatial.wkt(${shapeWktParameterName})");
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append(_shape);
        }
    }
}
