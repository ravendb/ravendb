using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

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
    
    public class SpatialProperty
    {
        public string LatitudeProperty;
        public string LongitudeProperty;

        public SpatialProperty()
        {
        }

        public SpatialProperty(string latitudePropertyPath, string longitudePropertyPath)
        {
            LatitudeProperty = latitudePropertyPath;
            LongitudeProperty = longitudePropertyPath;
        }
    }

    public enum SpatialShape
    {
        Polygon,
        Circle
    }

    public class LatLong
    {
        public double Latitude;
        public double Longitude;
        
        private LatLong()
        {
        }
        
        public LatLong(double latitude, double longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
        }
    } 

    public abstract class SpatialShapeBase
    {
        public SpatialShape ShapeType;
    }

    public class Polygon : SpatialShapeBase
    {
        public List<LatLong> Vertices;

        public Polygon()
        {
            ShapeType = SpatialShape.Polygon;
        }

        public Polygon(string polygonExpression) : this()
        {
            try
            {
                int indexStart = Regex.Matches(polygonExpression, "[(]")[1].Index + 1;
                int indexEnd = Regex.Matches(polygonExpression, "[)]")[0].Index;

                string pointsString = polygonExpression.Substring(indexStart, indexEnd - indexStart);
                var points = pointsString.Split(',');

                Vertices ??= new List<LatLong>();
                // Loop on all except the last one, since it is a duplicate of the first one.
                // This duplication is required only for the WKT format, but not for drawing polygon in Studio.
                for (var i = 0; i < points.Length-1; i++)
                {
                    var point = points[i].Split(default(string[]), StringSplitOptions.RemoveEmptyEntries);
                    // Revert long & lat - WKT format is [long, lat] and studio expects [lat, long]
                    var latitude = Convert.ToDouble(point[1]);
                    var longitude = Convert.ToDouble(point[0]);
                    Vertices.Add(new LatLong(latitude, longitude));
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("Invalid WKT POLYGON format. " + e.Message);
            }
        }
    }

    public class Circle : SpatialShapeBase
    {
        public LatLong Center;
        public double Radius;
        public SpatialUnits Units;
        
        public Circle()
        {
            ShapeType = SpatialShape.Circle;
        }

        public Circle(string radiusStr, string latitudeStr, string longitudeStr, string unitsStr) : this()
        {
            try
            {
                var latitude = Convert.ToDouble(latitudeStr);
                var longitude = Convert.ToDouble(longitudeStr);
                Center = new LatLong(latitude, longitude);
                Radius = Convert.ToDouble(radiusStr);
                Units = getUnits(unitsStr);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Invalid arguments in spatial.circle. " + e.Message);
            }
        }
        
        public Circle(string circleExpression, string unitsStr) : this()
        {
            try
            {
                var tokens = circleExpression.Split('(', ')');
                var circleItems = tokens[1].Split(default(string[]), StringSplitOptions.RemoveEmptyEntries);
                if (circleItems.Length != 3)
                {
                    throw new ArgumentException("WKT CIRCLE should contain 3 params. i.e. CIRCLE(longitude latitude d=radiusDistance)");
                }

                // Revert long & lat - WKT format is [long, lat] and studio expects [lat, long]
                var longitude = Convert.ToDouble(circleItems[0]);
                var latitude = Convert.ToDouble(circleItems[1]);
                Center = new LatLong(latitude, longitude);
                
                var radiusItems = circleItems[2].Split('=');
                if (radiusItems.Length != 2)
                {
                    throw new ArgumentException("Invalid radius distance param.");
                }
                Radius = Convert.ToDouble(radiusItems[1]);
                
                Units = getUnits(unitsStr);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Invalid WKT CIRCLE format. " + e.Message);
            }
        }

        private SpatialUnits getUnits(string unitsStr)
        {
            if (unitsStr?.ToLower() == "miles")
            {
                return SpatialUnits.Miles;
            }
            
            return SpatialUnits.Kilometers;
        }
    }
}
