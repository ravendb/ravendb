namespace Corax.Utils.Spatial;

public struct SpatialResult
{
    public double Distance;
    public double Latitude;
    public double Longitude;

    public static readonly SpatialResult Invalid = new() {Distance = double.NaN, Latitude = double.NaN, Longitude = double.NaN};
}
