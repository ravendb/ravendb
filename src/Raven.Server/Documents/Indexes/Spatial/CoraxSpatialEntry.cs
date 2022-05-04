using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Indexes.Spatial;

public struct CoraxSpatialEntry
{
    public double Latitude;

    public double Longitude;

    public List<string> Geohash;

    public string Wkt;

    public CoraxSpatialEntry(double latitude, double longitude, List<string> geohash)
    {
        Latitude = latitude;
        Longitude = longitude;
        Geohash = geohash;
        Unsafe.SkipInit(out Wkt);
    }
    
    public CoraxSpatialEntry(string wkt, List<string> geohash)
    {
        Wkt = wkt;
        Geohash = geohash;
        Unsafe.SkipInit(out Longitude);
        Unsafe.SkipInit(out Latitude);
    }
}

public enum CoraxSpatialEntryType
{
    LatLong,
    Wkt
}
