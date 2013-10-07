using System;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

namespace Raven.Studio.Features.Query
{
    public static class IndexQueryHelpers
    {
        public static IndexQuery FromQueryString(string queryString)
        {
			var fields = queryString.Split(new[] { '&' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(segment =>
                            {
								var parts = segment.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length == 1)
                                {
									return new { Key = parts[0], Value = string.Empty };
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
                IsDistinct = fields.IsDistinct(),
                SortedFields = fields["sort"]
                    .EmptyIfNull()
                    .Select(x => new SortedField(x))
                    .ToArray()
            };

            double lat = fields.GetLat(), lng = fields.GetLng(), radius = fields.GetRadius();
            SpatialUnits? units = fields.GetRadiusUnits();
            if (lat != 0 || lng != 0 || radius != 0)
            {
                return new SpatialIndexQuery(query)
                {
					QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(lat, lng, radius),
					RadiusUnitOverride = units,
                    SpatialRelation = SpatialRelation.Within, /* TODO */
					SpatialFieldName = Constants.DefaultSpatialFieldName, /* TODO */
                };
            }
            return query;
        }

		public static int GetStart(this ILookup<string, string> fields)
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

        public static bool IsDistinct(this ILookup<string, string> fields)
        {
	        var distinct = fields["distinct"].FirstOrDefault();
	        if (string.Equals("true", distinct, StringComparison.OrdinalIgnoreCase))
		        return true;
	        var aggAsString = fields["aggregation"].FirstOrDefault(); // 2.x legacy support

	        return string.Equals("Distinct", aggAsString, StringComparison.OrdinalIgnoreCase);
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

		public static Etag GetCutOffEtag(this ILookup<string, string> fields)
        {
            var etagAsString = fields["cutOffEtag"].FirstOrDefault();
            if (etagAsString != null)
            {
                etagAsString = Uri.UnescapeDataString(etagAsString);

				try
				{
					return Etag.Parse(etagAsString);
				}
				catch (Exception)
				{
                return null;
            }
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

        public static SpatialUnits? GetRadiusUnits(this ILookup<string, string> fields)
        {
            var units = fields["units"].FirstOrDefault();
            SpatialUnits parsedUnit;
            if (Enum.TryParse<SpatialUnits>(units, out parsedUnit))
            {
                return parsedUnit;
            }

            return null;
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
