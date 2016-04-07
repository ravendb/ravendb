using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    public static class IncludeUtil
    {
        private const char PrefixSeparator = '(';
        private static readonly char[] PrefixSeparatorChar = { PrefixSeparator };
        private static readonly BlittableJsonTraverser Traverser = new BlittableJsonTraverser();

        public static void GetDocIdFromInclude(BlittableJsonReaderObject docReader, StringSegment includePath, HashSet<string> includedIds)
        {
            var indexOfPrefixStart = includePath.IndexOfAny(PrefixSeparatorChar, 0);

            StringSegment pathSegment;
            StringSegment? prefix = null;

            if (indexOfPrefixStart != -1)
            {
                prefix = includePath.SubSegment(indexOfPrefixStart + 1);
                pathSegment = includePath.SubSegment(0, indexOfPrefixStart);
            }
            else
            {
                pathSegment = includePath;
            }

            object value;
            if (Traverser.TryRead(docReader, pathSegment, out value) == false)
                return;

            var collectionOfIds = value as IEnumerable;

            if (collectionOfIds != null)
            {
                foreach (var item in collectionOfIds)
                {
                    var id = prefix == null ? BlittableValueToString(item) : HandlePrefixValue(item, prefix.Value);
                    if (id != null)
                        includedIds.Add(id);
                }
            }
            else
            {
                var includedId = prefix == null ? BlittableValueToString(value) : HandlePrefixValue(value, prefix.Value);
                if (includedId != null)
                    includedIds.Add(includedId);
            }
        }

        private static string HandlePrefixValue(object val, StringSegment prefixSegment)
        {
            var doubleVal = val as LazyDoubleValue;
            if (doubleVal != null)
                val = doubleVal.Inner;

            return ValueWithPrefix(prefixSegment, val);
        }

        private static string ValueWithPrefix(StringSegment prefixSegment, object val)
        {
            var prefix = prefixSegment.SubSegment(0, prefixSegment.Length - 1);
            return (prefix.Length > 0) && (prefix[prefix.Length - 1] != '/') ? null : $"{prefix}{val}";
        }

        private static string BlittableValueToString(object value)
        {
            var lazyStringVal = value as LazyStringValue;
            if (lazyStringVal != null)
                return lazyStringVal.ToString();

            var lazyCompressedStringValue = value as LazyCompressedStringValue;
            if (lazyCompressedStringValue != null)
                return lazyCompressedStringValue.ToString();

            var lazyDoubleVal = value as LazyDoubleValue;
            if (lazyDoubleVal != null)
                return lazyDoubleVal.Inner.ToString();

            var convertible = value as IConvertible;
            return convertible?.ToString(CultureInfo.InvariantCulture);
        }

    }
}
