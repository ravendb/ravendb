using System;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Indexes
{
    internal static class FieldUtil
    {
        public static string RemoveRangeSuffixIfNecessary(string fieldName)
        {
            if (fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix) == false)
                return fieldName;

            var index = fieldName.Length - Constants.Documents.Indexing.Fields.RangeFieldSuffix.Length - 1;
            if (index < 1)
                return fieldName;

            var ch = fieldName[index];
            switch (ch)
            {
                case 'L':
                case 'D':
                    return fieldName.Substring(0, index - 1);
                default:
                    return fieldName;
            }
        }

        public static RangeType GetRangeTypeFromFieldName(string fieldName, out string originalFieldName)
        {
            if (fieldName.EndsWith(Constants.Documents.Indexing.Fields.RangeFieldSuffix))
            {
                var index = fieldName.Length - Constants.Documents.Indexing.Fields.RangeFieldSuffix.Length - 1;
                if (index < 1)
                {
                    originalFieldName = fieldName;
                    return RangeType.None;
                }

                var ch = fieldName[index];
                switch (ch)
                {
                    case 'L':
                        originalFieldName = fieldName.Substring(0, index - 1);
                        return RangeType.Long;
                    case 'D':
                        originalFieldName = fieldName.Substring(0, index - 1);
                        return RangeType.Double;
                    default:
                        throw new NotSupportedException($"Could not extract range type from '{fieldName}' field.");
                }
            }

            originalFieldName = fieldName;
            return RangeType.None;
        }

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