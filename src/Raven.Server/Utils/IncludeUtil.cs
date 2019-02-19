using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Extensions.Primitives;
using Raven.Server.Json;
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
        
        public interface IIncludeOp
        {
            void Include(BlittableJsonReaderObject parent, string id);
        }

        private struct HashSetIncludeOp : IIncludeOp
        {
            HashSet<string> _items;

            public HashSetIncludeOp(HashSet<string> items)
            {
                _items = items;
            }

            public void Include(BlittableJsonReaderObject parent, string id)
            {
                _items.Add(id);
            }
        }

        public static void GetDocIdFromInclude(BlittableJsonReaderObject docReader, StringSegment includePath,
            HashSet<string> includedIds)
        {
            var op = new HashSetIncludeOp(includedIds);
            GetDocIdFromInclude(docReader, includePath, op);
        }

        public static void GetDocIdFromInclude<TIncludeOp>(BlittableJsonReaderObject docReader, StringSegment includePath, TIncludeOp op)
            where TIncludeOp : struct, IIncludeOp
        {
            Func<object, StringSegment, string> valueHandler = null;

            var indexOfPrefixStart = includePath.IndexOfAny(PrefixSeparatorChar, 0);

            StringSegment pathSegment;
            StringSegment? addition = null;

            if (HasSuffixSeparator(includePath, out var indexOfSuffixStart))
            {
                addition = includePath.Subsegment(indexOfSuffixStart + 1);

                if (!addition.Value[addition.Value.Length - 1].Equals(']') ||
                    ((addition.Value.Length >= 4) &&
                     !addition.Value.Subsegment(0, 4).Equals(SuffixStart)))
                    return;
                pathSegment = includePath.Subsegment(0, indexOfSuffixStart);
                valueHandler = HandleSuffixValue;
            }
            else if (indexOfPrefixStart != -1)
            {
                addition = includePath.Subsegment(indexOfPrefixStart + 1);
                if (!includePath[includePath.Length - 1].Equals(')'))
                    return;
                pathSegment = includePath.Subsegment(0, indexOfPrefixStart);
                valueHandler = HandlePrefixValue;
            }
            else
            {
                pathSegment = includePath;
            }

            if (BlittableJsonTraverser.Default.TryRead(docReader, pathSegment, out object value, out StringSegment leftPath) == false)
            {
                var json = value as BlittableJsonReaderObject;
                if (json != null)
                {
                    var isKey = leftPath == "$Keys"; // include on dictionary.Keys or dictionary.Values
                    if (isKey || leftPath == "$Values")
                    {
                        var property = new BlittableJsonReaderObject.PropertyDetails();
                        for (int i = 0; i < json.Count; i++)
                        {
                            json.GetPropertyByIndex(i, ref property);
                            var val = isKey ? property.Name : property.Value;
                            if (val != null)
                            {
                                op.Include(null, val.ToString());
                            }
                        }
                    }
                }
                if(value is BlittableJsonReaderArray array)
                {
                    foreach (var item in array)
                    {
                        if (item is BlittableJsonReaderObject inner)
                            GetDocIdFromInclude(inner, leftPath, op);
                    }
                }

                return;
            }

            var collectionOfIds = value as IEnumerable;

            if (collectionOfIds != null)
            {
                foreach (var item in collectionOfIds)
                {
                    if (item == null)
                        continue;
                    if (addition != null)
                    {
                        var includedId = valueHandler(item, addition.Value);
                        if (includedId != null)
                            op.Include(null, includedId);
                    }
                    op.Include(null, BlittableValueToString(item));
                }
            }
            else
            {
                if (addition != null)
                {
                    var includedId = valueHandler(value, addition.Value);
                    if (includedId != null)
                        op.Include(docReader, includedId);
                }
                op.Include(docReader, BlittableValueToString(value));
            }
        }

        private static bool HasSuffixSeparator(StringSegment includePath, out int indexOfPrefixStart)
        {
            indexOfPrefixStart = includePath.LastIndexOf(SuffixSeparator);

            if (indexOfPrefixStart == -1)
                return false;

            if (includePath.Length >= indexOfPrefixStart + BlittableJsonTraverser.CollectionAndPropertySeparators.Length)
            {
                if (includePath[indexOfPrefixStart] == BlittableJsonTraverser.CollectionAndPropertySeparators[0] &&
                    includePath[indexOfPrefixStart + 1] == BlittableJsonTraverser.CollectionAndPropertySeparators[1] &&
                    includePath[indexOfPrefixStart + 2] == BlittableJsonTraverser.CollectionAndPropertySeparators[2])
                {
                     // []. means collection

                    return false;
                }
            }

            return true;
        }

        private static string HandleSuffixValue(object val, StringSegment suffixSegment)
        {
            var doubleVal = val as LazyNumberValue;
            if (doubleVal != null)
                val = doubleVal.Inner;
            var res = string.Format(suffixSegment.Value, val).TrimEnd(']');
            return res == "" ? null : res;
        }

        private static string HandlePrefixValue(object val, StringSegment prefixSegment)
        {
            var doubleVal = val as LazyNumberValue;
            if (doubleVal != null)
                val = doubleVal.Inner;

            return ValueWithPrefix(prefixSegment, val);
        }

        private static string ValueWithPrefix(StringSegment prefixSegment, object val)
        {
            var prefix = prefixSegment.Subsegment(0, prefixSegment.Length - 1);
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

            var lazyDoubleVal = value as LazyNumberValue;
            if (lazyDoubleVal != null)
                return lazyDoubleVal.Inner.ToString();

            var convertible = value as IConvertible;
            return convertible?.ToString(CultureInfo.InvariantCulture);
        }

    }
}
