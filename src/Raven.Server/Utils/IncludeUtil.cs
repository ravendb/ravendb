using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    public static class IncludeUtil
    {
        private const char PrefixSeparator = '(';
        private const char SuffixSeparator = '[';
        private const string SuffixStart = "{0}/";
        private static readonly char[] PrefixSeparatorChar = { PrefixSeparator };
        private static readonly char[] SuffixSeparatorChar = { SuffixSeparator };
        private static Func<object, StringSegment, string> ValueHandler;

        public static void GetDocIdFromInclude(BlittableJsonReaderObject docReader, StringSegment includePath,
            HashSet<string> includedIds)
        {
            var indexOfPrefixStart = includePath.IndexOfAny(PrefixSeparatorChar, 0);
            var indexOfSuffixStart = includePath.IndexOfAny(SuffixSeparatorChar, 0);

            StringSegment pathSegment;
            StringSegment? addition = null;

            if (indexOfSuffixStart != -1)
            {
                addition = includePath.SubSegment(indexOfSuffixStart + 1);
                if (!addition.Value[addition.Value.Length - 1].Equals(']') ||
                    ((addition.Value.Length >= 4) &&
                     !addition.Value.SubSegment(0, 4).Equals(SuffixStart)))
                    return;
                pathSegment = includePath.SubSegment(0, indexOfSuffixStart);
                ValueHandler = HandleSuffixValue;
            }
            else if (indexOfPrefixStart != -1)
            {
                addition = includePath.SubSegment(indexOfPrefixStart + 1);
                if (!includePath[includePath.Length - 1].Equals(')'))
                    return;
                pathSegment = includePath.SubSegment(0, indexOfPrefixStart);
                ValueHandler = HandlePrefixValue;
            }
            else
            {
                pathSegment = includePath;
            }

            object value;
            StringSegment leftPath;
            if (BlittableJsonTraverser.Default.TryRead(docReader, pathSegment, out value, out leftPath) == false)
                return;

            var collectionOfIds = value as IEnumerable;

            if (collectionOfIds != null)
            {
                foreach (var item in collectionOfIds)
                {
                    if (addition != null)
                    {
                        var includedId = ValueHandler(item, addition.Value);
                        if (includedId != null)
                            includedIds.Add(includedId);
                    }
                    includedIds.Add(BlittableValueToString(item));
                }
            }
            else
            {
                if (addition != null)
                {
                    var includedId = ValueHandler(value, addition.Value);
                    if (includedId != null)
                        includedIds.Add(includedId);
                }
                includedIds.Add(BlittableValueToString(value));
            }
        }

        private static string HandleSuffixValue(object val, StringSegment suffixSegment)
        {
            var doubleVal = val as LazyDoubleValue;
            if (doubleVal != null)
                val = doubleVal.Inner;
            var res = string.Format(suffixSegment, val).TrimEnd(']');
            return res == "" ? null : res;
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
