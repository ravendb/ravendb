using System;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Server.Documents.Indexes.Spatial
{
    public class Circle : SpatialShapeBase
    {
        public Coordinates Center;
        public double Radius;
        public SpatialUnits Units;

        public Circle()
        {
            ShapeType = SpatialShape.Circle;
        }

        public Circle(string radiusStr, string latitudeStr, string longitudeStr, string unitsStr) : this()
        {
            try
            {
                var latitude = Convert.ToDouble(latitudeStr);
                var longitude = Convert.ToDouble(longitudeStr);
                Center = new Coordinates(latitude, longitude);
                Radius = Convert.ToDouble(radiusStr);
                Units = GetUnits(unitsStr);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Invalid arguments in spatial.circle. " + e.Message);
            }
        }

        public Circle(string circleExpression, string unitsStr) : this()
        {
            try
            {
                var tokens = circleExpression.Split('(', ')');
                var circleItems = tokens[1].Split(default(string[]), StringSplitOptions.RemoveEmptyEntries);
                if (circleItems.Length != 3)
                {
                    throw new ArgumentException("WKT CIRCLE should contain 3 params. i.e. CIRCLE(longitude latitude d=radiusDistance)");
                }

                // Revert long & lat - WKT format is [long, lat] and studio expects [lat, long]
                var longitude = Convert.ToDouble(circleItems[0]);
                var latitude = Convert.ToDouble(circleItems[1]);
                Center = new Coordinates(latitude, longitude);

                var radiusItems = circleItems[2].Split('=');
                if (radiusItems.Length != 2)
                {
                    throw new ArgumentException("Invalid radius distance param.");
                }
                Radius = Convert.ToDouble(radiusItems[1]);

                Units = GetUnits(unitsStr);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Invalid WKT CIRCLE format. " + e.Message);
            }
        }

        private SpatialUnits GetUnits(string unitsStr)
        {
            if (unitsStr?.ToLower() == "miles")
            {
                return SpatialUnits.Miles;
            }

            return SpatialUnits.Kilometers;
        }
    }
}
