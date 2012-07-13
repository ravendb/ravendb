using System.Collections.Generic;
using Raven.Client.Silverlight.MissingFromSilverlight;
using System.Linq;

namespace Raven.Studio.Features.Query
{
    public class QueryState
    {
        public QueryState(string indexName, string query, IEnumerable<string> sortOptions, bool isSpatialQuery, double? latitude, double? longitude, double? radius)
        {
            IndexName = indexName;
            Query = query;
            IsSpatialQuery = isSpatialQuery;
            Latitude = latitude;
            Longitude = longitude;
            Radius = radius;
            SortOptions = sortOptions.ToList();
        }

        public string IndexName { get; private set; }

        public string Query { get; private set; }
        public bool IsSpatialQuery { get; private set; }
        public double? Latitude { get; private set; }
        public double? Longitude { get; private set; }
        public double? Radius { get; private set; }

        public IList<string> SortOptions { get; private set; }

        public string GetHash()
        {
            return CreateQueryStateHash(IndexName, Query, IsSpatialQuery, Latitude, Longitude, Radius);
        }

        public static string CreateQueryStateHash(string indexName, string query, bool isSpatial, double? latitude, double? longtitude, double? radius)
        {
            var spatialString = isSpatial ? string.Format("{0}{1}{2}", latitude, longtitude, radius) : "";
            return MD5Core.GetHashString(indexName + query + spatialString);
        }
    }
}