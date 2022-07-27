using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;
using Raven.Server.Config.Categories;

namespace Raven.Server.Utils
{
    public static class IncludeUtil
    {
        private const char PrefixSeparator = '(';
        private const char SuffixSeparator = '[';
        private const string SuffixStart = "{0}";
        private static readonly char[] PrefixSeparatorChar = { PrefixSeparator };

        public interface IIncludeOp
        {
            void Include(BlittableJsonReaderObject parent, string id);
        }

        private struct HashSetIncludeOp : IIncludeOp
        {
            private HashSet<string> _items;

            public HashSetIncludeOp(HashSet<string> items)
            {
                _items = items;
            }

            public void Include(BlittableJsonReaderObject parent, string id)
            {
                _items.Add(id);
            }
        }

        public static void GetDocIdFromInclude(
            BlittableJsonReaderObject docReader, 
            StringSegment includePath, 
            HashSet<string> includedIds, 
            char identityPartsSeparator)
        {
            var op = new HashSetIncludeOp(includedIds);
            GetDocIdFromInclude(docReader, includePath, identityPartsSeparator, op);
        }

        public static void GetDocIdFromInclude<TIncludeOp>(
            BlittableJsonReaderObject docReader, 
            StringSegment includePath, 
            char identityPartsSeparator, 
            TIncludeOp op)
            where TIncludeOp : struct, IIncludeOp
        {
            Func<object, StringSegment, char, string> valueHandler = null;

            var indexOfPrefixStart = includePath.IndexOfAny(PrefixSeparatorChar, 0);

            StringSegment pathSegment;
            StringSegment? addition = null;

            if (HasSuffixSeparator(includePath, out var indexOfSuffixStart))
            {
                addition = includePath.Subsegment(indexOfSuffixStart + 1);

                if (addition.Value[addition.Value.Length - 1].Equals(']') == false 
                    || ((addition.Value.Length >= 4) && (addition.Value.Subsegment(0, 3).Equals(SuffixStart) == false || addition.Value[3].Equals(identityPartsSeparator) == false)))
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
                    // include on dictionary.Keys or dictionary.Values
                    var isKey = leftPath == "$Keys" || leftPath == "$Key";
                    var isValue = leftPath == "$Values" || leftPath == "$Value";
                    if (isKey || isValue)
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
                if (value is BlittableJsonReaderArray array)
                {
                    foreach (var item in array)
                    {
                        if (item is BlittableJsonReaderObject inner)
                            GetDocIdFromInclude(inner, leftPath, identityPartsSeparator, op);
                    }
                }

                return;
            }

            var collectionOfIds = value as IEnumerable;

            if (collectionOfIds != null && value is LazyStringValue == false)
            {
                foreach (var item in collectionOfIds)
                {
                    if (item == null)
                        continue;
                    if (addition != null)
                    {
                        var includedId = valueHandler(item, addition.Value, identityPartsSeparator);
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
                    var includedId = valueHandler(value, addition.Value, identityPartsSeparator);
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

        private static string HandleSuffixValue(object val, StringSegment suffixSegment, char identityPartsSeparator)
        {
            var doubleVal = val as LazyNumberValue;
            if (doubleVal != null)
                val = doubleVal.Inner;
            var res = string.Format(suffixSegment.Value, val).TrimEnd(']');
            return res == string.Empty ? null : res;
        }

        private static string HandlePrefixValue(object val, StringSegment prefixSegment, char identityPartsSeparator)
        {
            var doubleVal = val as LazyNumberValue;
            if (doubleVal != null)
                val = doubleVal.Inner;

            return ValueWithPrefix(val, prefixSegment, identityPartsSeparator);
        }

        private static string ValueWithPrefix(object val, StringSegment prefixSegment, char identityPartsSeparator)
        {
            var prefix = prefixSegment.Subsegment(0, prefixSegment.Length - 1);
            return (prefix.Length > 0) && (prefix[prefix.Length - 1] != identityPartsSeparator) ? null : $"{prefix}{val}";
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
