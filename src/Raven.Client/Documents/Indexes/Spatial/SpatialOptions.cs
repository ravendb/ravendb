using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Documents.Indexes.Spatial
{
    public class AutoSpatialOptions : SpatialOptions
    {
        public AutoSpatialMethodType MethodType { get; set; }

        public List<string> MethodArguments { get; set; }

        public AutoSpatialOptions()
        {
            MethodArguments = new List<string>();
        }

        public AutoSpatialOptions(AutoSpatialMethodType methodType, List<string> methodArguments)
        {
            MethodType = methodType;
            MethodArguments = methodArguments ?? throw new ArgumentNullException(nameof(methodArguments));
        }

        public AutoSpatialOptions(AutoSpatialOptions options)
            : base(options)
        {
            MethodType = options.MethodType;
            MethodArguments = new List<string>(options.MethodArguments);
        }

        protected bool Equals(AutoSpatialOptions other)
        {
            return base.Equals(other) && MethodType == other.MethodType && MethodArguments.SequenceEqual(other.MethodArguments);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((AutoSpatialOptions)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)MethodType;
                hashCode = (hashCode * 397) ^ (MethodArguments != null ? MethodArguments.GetHashCode() : 0);
                return hashCode;
            }
        }

        public enum AutoSpatialMethodType
        {
            Point,
            Wkt
        }
    }

    public class SpatialOptions
    {
        // about 4.78 meters at equator, should be good enough (see: http://unterbahn.com/2009/11/metric-dimensions-of-geohash-partitions-at-the-equator/)
        public const int DefaultGeohashLevel = 9;

        // about 4.78 meters at equator, should be good enough
        public const int DefaultQuadTreeLevel = 23;

        internal static readonly SpatialOptions Default = new SpatialOptions();

        public SpatialOptions()
        {
            Type = SpatialFieldType.Geography;
            Strategy = SpatialSearchStrategy.GeohashPrefixTree;
            MaxTreeLevel = DefaultGeohashLevel;
            MinX = -180;
            MaxX = 180;
            MinY = -90;
            MaxY = 90;
            Units = SpatialUnits.Kilometers;
        }

        internal SpatialOptions(SpatialOptions options)
        {
            Type = options.Type;
            Strategy = options.Strategy;
            MaxTreeLevel = options.MaxTreeLevel;
            MinX = options.MinX;
            MaxX = options.MaxX;
            MinY = options.MinY;
            MaxY = options.MaxY;
            Units = options.Units;
        }

        public SpatialFieldType Type { get; set; }
        public SpatialSearchStrategy Strategy { get; set; }
        public int MaxTreeLevel { get; set; }
        public double MinX { get; set; }
        public double MaxX { get; set; }
        public double MinY { get; set; }
        public double MaxY { get; set; }

        /// <summary>
        /// Circle radius units, only used for geography indexes
        /// </summary>
        public SpatialUnits Units { get; set; }

        protected bool Equals(SpatialOptions other)
        {
            var result = Type == other.Type && Strategy == other.Strategy;

            if (Type == SpatialFieldType.Geography)
            {
                result = result && Units == other.Units;
            }

            if (Strategy != SpatialSearchStrategy.BoundingBox)
            {
                result = result && MaxTreeLevel == other.MaxTreeLevel;

                if (Type == SpatialFieldType.Cartesian)
                {
                    result = result
                        && MinX.Equals(other.MinX)
                        && MaxX.Equals(other.MaxX)
                        && MinY.Equals(other.MinY)
                        && MaxY.Equals(other.MaxY);
                }
            }
            return result;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((SpatialOptions)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (int)Type;
                hashCode = (hashCode * 397) ^ (int)Strategy;

                if (Type == SpatialFieldType.Geography)
                {
                    hashCode = (hashCode * 397) ^ Units.GetHashCode();
                }

                if (Strategy != SpatialSearchStrategy.BoundingBox)
                {
                    hashCode = (hashCode * 397) ^ MaxTreeLevel;

                    if (Type == SpatialFieldType.Cartesian)
                    {
                        hashCode = (hashCode * 397) ^ MinX.GetHashCode();
                        hashCode = (hashCode * 397) ^ MaxX.GetHashCode();
                        hashCode = (hashCode * 397) ^ MinY.GetHashCode();
                        hashCode = (hashCode * 397) ^ MaxY.GetHashCode();
                    }
                }

                return hashCode;
            }
        }
    }

    public enum SpatialFieldType
    {
        Geography,
        Cartesian
    }

    public enum SpatialSearchStrategy
    {
        GeohashPrefixTree,
        QuadPrefixTree,
        BoundingBox
    }

    public enum SpatialRelation
    {
        Within,
        Contains,
        Disjoint,
        Intersects
    }

    public enum SpatialUnits
    {
        Kilometers,
        Miles
    }
}
