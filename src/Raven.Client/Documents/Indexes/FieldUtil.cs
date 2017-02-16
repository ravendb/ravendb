using System;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Indexes
{
    internal static class FieldUtil
    {
        public static string ApplyRangeSuffixIfNecessary(string fieldName, object @object)
        {
            var rangeType = DocumentConventions.GetRangeType(@object);
            return ApplyRangeSuffixIfNecessary(fieldName, rangeType);
        }

        public static string ApplyRangeSuffixIfNecessary(string fieldName, Type type)
        {
            var rangeType = DocumentConventions.GetRangeType(type);
            return ApplyRangeSuffixIfNecessary(fieldName, rangeType);
        }

        public static string ApplyRangeSuffixIfNecessary(string fieldName, RangeType rangeType)
        {
            if (rangeType == RangeType.None)
                return fieldName;

            if (fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffixLong) || fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble))
                return fieldName;

            if (rangeType == RangeType.Long)
                return fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;

            return fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
        }
    }
}