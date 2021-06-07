using System;
using System.Collections.Generic;
using GeoAPI.Geometries;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Spatial
{
    public class Polygon : SpatialShapeBase
    {
        public readonly List<Coordinates> Vertices;

        public Polygon(IRectangle rectangle) : base(SpatialShapeType.Polygon)
        {
            if (rectangle == null)
                throw new ArgumentNullException(nameof(rectangle));

            Vertices ??= new List<Coordinates>();

            Vertices.Add(new Coordinates(rectangle.MaxY, rectangle.MaxX));
            Vertices.Add(new Coordinates(rectangle.MaxY, rectangle.MinY));
            Vertices.Add(new Coordinates(rectangle.MinY, rectangle.MaxX));
            Vertices.Add(new Coordinates(rectangle.MinY, rectangle.MinX));
        }
        
        public Polygon(NetTopologySuite.Geometries.Polygon polygon) : base(SpatialShapeType.Polygon)
        {
            if (polygon == null)
                throw new ArgumentNullException(nameof(polygon));
            
            Vertices ??= new List<Coordinates>();
            
            foreach (Coordinate coordinate in polygon.Coordinates)
                Vertices.Add(new Coordinates(coordinate.Y, coordinate.X));
        }
    }
}
