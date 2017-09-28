using System;

namespace Raven.Client.Documents.Queries.Spatial
{
    public class PointField : SpatialDynamicField
    {
        public readonly string Latitude;
        public readonly string Longitude;

        public PointField(string latitude, string longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
        }

        public override string ToField(Func<string, bool, string> ensureValidFieldName)
        {
            return $"point({ensureValidFieldName(Latitude, false)}, {ensureValidFieldName(Longitude, false)})";
        }
    }

    public class WktField : SpatialDynamicField
    {
        public readonly string Wkt;

        public WktField(string wkt)
        {
            Wkt = wkt;
        }

        public override string ToField(Func<string, bool, string> ensureValidFieldName)
        {
            return $"wkt({ensureValidFieldName(Wkt, false)})";
        }
    }

    public abstract class SpatialDynamicField
    {
        public abstract string ToField(Func<string, bool, string> ensureValidFieldName);
    }
}
