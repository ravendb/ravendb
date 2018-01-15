using Lucene.Net.Util;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;

namespace Raven.Server.Documents.Queries.Facets
{
    public static class FacetedQueryHelper
    {
        public static string GetRangeName(string field, string text)
        {
            var rangeType = FieldUtil.GetRangeTypeFromFieldName(field);
            switch (rangeType)
            {
                case RangeType.Long:
                    return NumericUtils.PrefixCodedToLong(text).ToInvariantString();
                case RangeType.Double:
                    return NumericUtils.PrefixCodedToDouble(text).ToInvariantString();
                default:
                    return text;
            }
        }
    }
}
