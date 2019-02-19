using System;

namespace Raven.Client.Documents.Indexes
{
    internal static class FieldUtil
    {
        public static RangeType GetRangeTypeFromFieldName(string fieldName)
        {
            if (fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix))
            {
                var index = fieldName.Length - Constants.Documents.Indexing.Fields.RangeFieldSuffix.Length - 1;
                if (index < 1)
                {
                    return RangeType.None;
                }

                var ch = fieldName[index];
                switch (ch)
                {
                    case 'L':
                        return RangeType.Long;
                    case 'D':
                        return RangeType.Double;
                    default:
                        throw new InvalidOperationException($"Client does not support '{Constants.Documents.Indexing.Fields.RangeFieldSuffix}' suffix any longer. Please use '{Constants.Documents.Indexing.Fields.RangeFieldSuffixLong}' or '{Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble}' suffixes.");
                }
            }

            return RangeType.None;
        }

        public static string ApplyRangeSuffixIfNecessary(string fieldName, RangeType rangeType)
        {
            if (rangeType == RangeType.None)
                return fieldName;

            if (fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix))
            {
                var index = fieldName.Length - Constants.Documents.Indexing.Fields.RangeFieldSuffix.Length - 1;
                var ch = fieldName[index];
                if (ch == 'L' || ch == 'D')
                    return fieldName;

                throw new InvalidOperationException($"Client does not support '{Constants.Documents.Indexing.Fields.RangeFieldSuffix}' suffix any longer. Please use '{Constants.Documents.Indexing.Fields.RangeFieldSuffixLong}' or '{Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble}' suffixes.");
            }

            if (rangeType == RangeType.Long)
                return fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;

            return fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
        }
    }
}
