using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;

namespace Raven.Studio.Features.Query
{
    public static class IndexQueryHelpers
    {
        public static IndexQuery FromQueryString(string queryString)
        {
            var fields = queryString.Split(new[] {'&'}, StringSplitOptions.RemoveEmptyEntries)
                .Select(segment =>
                            {
                                var parts = segment.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length == 1)
                                {
                                    return new {Key = parts[0], Value = string.Empty};
                                }
                                else
                                {
                                    return
                                        new
                                            {
                                                Key = parts[0],
                                                Value = Uri.UnescapeDataString(parts[1])
                                            };
                                }
                            }).ToLookup(f => f.Key, f => f.Value);

            var query = new IndexQuery
            {
                Query = Uri.UnescapeDataString(fields["query"].FirstOrDefault() ?? ""),
                Start = fields.GetStart(),
                Cutoff = fields.GetCutOff(),
                CutoffEtag = fields.GetCutOffEtag(),
                PageSize = fields.GetPageSize(250),
                SkipTransformResults = fields.GetSkipTransformResults(),
                FieldsToFetch = fields["fetch"].ToArray(),
                GroupBy = fields["groupBy"].ToArray(),
                AggregationOperation = fields.GetAggregationOperation(),
                SortedFields = fields["sort"]
                    .EmptyIfNull()
                    .Select(x => new SortedField(x))
                    .ToArray()
            };

            double lat = fields.GetLat(), lng = fields.GetLng(), radius = fields.GetRadius();
            if (lat != 0 || lng != 0 || radius != 0)
            {
                return new SpatialIndexQuery(query)
                {
                    Latitude = lat,
                    Longitude = lng,
                    Radius = radius,
                };
            }
            return query;
        }

        public static int GetStart(this ILookup<string,string> fields)
        {
            int start;
            int.TryParse(fields["start"].FirstOrDefault(), out start);
            return Math.Max(0, start);
        }

        public static bool GetSkipTransformResults(this ILookup<string, string> fields)
        {
            bool result;
            bool.TryParse(fields["skipTransformResults"].FirstOrDefault(), out result);
            return result;
        }

        public static int GetPageSize(this ILookup<string, string> fields, int maxPageSize)
        {
            int pageSize;
            if (int.TryParse(fields["pageSize"].FirstOrDefault(), out pageSize) == false || pageSize < 0)
                pageSize = 25;
            if (pageSize > maxPageSize)
                pageSize = maxPageSize;
            return pageSize;
        }

        public static AggregationOperation GetAggregationOperation(this ILookup<string, string> fields)
        {
            var aggAsString = fields["aggregation"].FirstOrDefault();
            if (aggAsString == null)
            {
                return AggregationOperation.None;
            }

            return (AggregationOperation)Enum.Parse(typeof(AggregationOperation), aggAsString, true);
        }

        public static DateTime? GetCutOff(this ILookup<string, string> fields)
        {
            var etagAsString = fields["cutOff"].FirstOrDefault();
            if (etagAsString != null)
            {
                etagAsString = Uri.UnescapeDataString(etagAsString);

                DateTime result;
                if (DateTime.TryParseExact(etagAsString, "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
                    return result;
                return null;
            }
            return null;
        }

        public static Guid? GetCutOffEtag(this ILookup<string, string> fields)
        {
            var etagAsString = fields["cutOffEtag"].FirstOrDefault();
            if (etagAsString != null)
            {
                etagAsString = Uri.UnescapeDataString(etagAsString);

                Guid result;
                if (Guid.TryParse(etagAsString, out result))
                    return result;
                return null;
            }
            return null;
        }

        public static double GetLat(this ILookup<string, string> fields)
        {
            double lat;
            double.TryParse(fields["latitude"].FirstOrDefault(), NumberStyles.Any, CultureInfo.InvariantCulture, out lat);
            return lat;
        }

        public static double GetLng(this ILookup<string, string> fields)
        {
            double lng;
            double.TryParse(fields["longitude"].FirstOrDefault(), NumberStyles.Any, CultureInfo.InvariantCulture, out lng);
            return lng;
        }

        public static double GetRadius(this ILookup<string, string> fields)
        {
            double radius;
            double.TryParse(fields["radius"].FirstOrDefault(), NumberStyles.Any, CultureInfo.InvariantCulture, out radius);
            return radius;
        }

        public static Guid? GetEtagFromQueryString(this ILookup<string, string> fields)
        {
            var etagAsString = fields["etag"].FirstOrDefault();
            if (etagAsString != null)
            {
                Guid result;
                if (Guid.TryParse(etagAsString, out result))
                    return result;
                return null;
            }
            return null;
        }

        
    }
}
