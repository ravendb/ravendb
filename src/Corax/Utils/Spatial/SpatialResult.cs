using System;
using System.Runtime.InteropServices;

namespace Corax.Utils.Spatial;

[StructLayout(LayoutKind.Sequential)]
public struct SpatialResult : IComparable
{
    public double Distance;
    public double Latitude;
    public double Longitude;

    public static readonly SpatialResult Invalid = new() {Distance = double.NaN, Latitude = double.NaN, Longitude = double.NaN};

    public int CompareTo(object other)
    {
        return Distance.CompareTo(((SpatialResult)other!).Distance);
    }
}
