using System;
using Sparrow;

namespace Corax.Utils;

public readonly struct CoraxSpatialPointEntry
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
}
