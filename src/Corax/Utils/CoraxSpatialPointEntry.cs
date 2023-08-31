using System;

namespace Corax.Utils;

public sealed class CoraxSpatialPointEntry
{
    public readonly double Latitude;

    public readonly double Longitude;

    public readonly string Geohash;

    public CoraxSpatialPointEntry(double latitude, double longitude, string geohash)
    {
        Latitude = latitude;
        Longitude = longitude;
        Geohash = geohash;
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(this ,obj))
            return true;
        if (obj is CoraxSpatialPointEntry csp == false)
            return false;
        return Latitude.Equals(csp.Latitude) && Longitude.Equals(csp.Longitude) && Geohash == csp.Geohash;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Latitude, Longitude, Geohash);
    }
}
