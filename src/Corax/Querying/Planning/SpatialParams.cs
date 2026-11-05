namespace Corax.Querying.Planning;

public sealed class SpatialParams
{
    public double DistanceErrorPct = -1; // -1 = use default
    public SpatialShapeType ShapeType;
    // Circle parameters
    public double CircleRadius;
    public double CircleLatitude;
    public double CircleLongitude;
    // WKT parameter
    public string Wkt;
    public Utils.Spatial.SpatialUnits? Units;
}
