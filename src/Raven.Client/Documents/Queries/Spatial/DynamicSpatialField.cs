using System;

namespace Raven.Client.Documents.Queries.Spatial
{
    public class PointField : DynamicSpatialField
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
            return $"spatial.point({ensureValidFieldName(Latitude, false)}, {ensureValidFieldName(Longitude, false)})";
        }
    }

    public class WktField : DynamicSpatialField
    {
        public readonly string Wkt;

        public WktField(string wkt)
        {
            Wkt = wkt;
        }

        public override string ToField(Func<string, bool, string> ensureValidFieldName)
        {
            return $"spatial.wkt({ensureValidFieldName(Wkt, false)})";
        }
    }

    public abstract class DynamicSpatialField
    {
        public abstract string ToField(Func<string, bool, string> ensureValidFieldName);

        public double RoundFactor;

        public DynamicSpatialField RoundTo(double factor)
        {
            RoundFactor = factor;
            return this;
        }
    }
}
