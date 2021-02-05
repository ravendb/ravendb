using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Server.Documents.Indexes.Spatial
{
    public class Polygon : SpatialShapeBase
    {
        public List<Coordinates> Vertices;

        public Polygon()
        {
            ShapeType = SpatialShape.Polygon;
        }

        public Polygon(string polygonExpression) : this()
        {
            try
            {
                int indexStart = Regex.Matches(polygonExpression, "[(]")[1].Index + 1;
                int indexEnd = Regex.Matches(polygonExpression, "[)]")[0].Index;

                string pointsString = polygonExpression.Substring(indexStart, indexEnd - indexStart);
                var points = pointsString.Split(',');

                Vertices ??= new List<Coordinates>();
                // Loop on all except the last one, since it is a duplicate of the first one.
                // This duplication is required only for the WKT format, but not for drawing polygon in Studio.
                for (var i = 0; i < points.Length - 1; i++)
                {
                    var point = points[i].Split(default(string[]), StringSplitOptions.RemoveEmptyEntries);
                    // Revert long & lat - WKT format is [long, lat] and studio expects [lat, long]
                    var latitude = Convert.ToDouble(point[1]);
                    var longitude = Convert.ToDouble(point[0]);
                    Vertices.Add(new Coordinates(latitude, longitude));
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("Invalid WKT POLYGON format. " + e.Message);
            }
        }
    }
}
