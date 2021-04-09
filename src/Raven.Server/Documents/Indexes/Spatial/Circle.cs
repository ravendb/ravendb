using System;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Spatial
{
    public class Circle : SpatialShapeBase
    {
        public readonly Coordinates Center;
        public readonly double Radius;
        public readonly SpatialUnits Units;

        public Circle(ICircle circle, SpatialUnits units, SpatialOptions options)
            : base(SpatialShapeType.Circle)
        {
            if (circle == null)
                throw new ArgumentNullException(nameof(circle));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            Center = new Coordinates(circle.Center);
            Radius = ShapeStringReadWriter.TranslateDegreesToRadius(circle.Radius, units, options);
            Units = units;
        }
    }
}
