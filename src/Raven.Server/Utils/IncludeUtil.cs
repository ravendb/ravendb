using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Utils
{
    public static class IncludeUtil
    {
        private static readonly char[] IncludeSeparators = { '.', ',', '(' };

        public static void GetDocIdFromInclude(BlittableJsonReaderObject docReader, StringSegment includePath, HashSet<string> includedIds)
        {
            var indexOfFirstSeparator = includePath.IndexOfAny(IncludeSeparators, 0);
            object reader;
            if (docReader.TryGetMember(includePath.SubSegment(0, indexOfFirstSeparator), out reader) == false)
                return;

            var pathSegment = includePath.SubSegment(indexOfFirstSeparator + 1);
            switch (indexOfFirstSeparator != -1 ? includePath[indexOfFirstSeparator] : ' ')
            {
                case '.':
                    var subObject = reader as BlittableJsonReaderObject;
                    if(subObject != null)
                        GetDocIdFromInclude(subObject, pathSegment, includedIds);
                    return;
                case ',':
                    var subArray = reader as BlittableJsonReaderArray;
                    if (subArray != null)
                    {
                        for (int i = 0; i < subArray.Length; i++)
                        {
                            var item = subArray[i];
                            var arrayObject = item as BlittableJsonReaderObject;
                            if (arrayObject != null)
                            {
                                GetDocIdFromInclude(arrayObject, pathSegment, includedIds);
                            }
                            else
                            {
                                //TODO: handle array
                                //TODO: handle simple value
                            }
                        }
                    }
                    break;
                case '(':
                    if (includePath[includePath.Length - 1] != ')') //precaution
                        return;

                    includedIds.Add(HandlePrefix(docReader, includePath.SubSegment(0, indexOfFirstSeparator), indexOfFirstSeparator));
                    break;
                default:
                    object value;
                    if (docReader.TryGetMember(includePath, out value))
                    {
                        var includedId = BlittableValueToString(value);
                        if(includedId != null)
                            includedIds.Add(includedId);
                    }
                    return;
            }			
        }

        private static string HandlePrefix(BlittableJsonReaderObject reader, StringSegment pathSegment, int indexOfSeparator)
        {
            object val;
            if (!reader.TryGetMember(pathSegment.SubSegment(0, indexOfSeparator), out val))
                return null;
        
            var doubleVal = val as LazyDoubleValue;
            if (doubleVal != null)
                val = doubleVal.Inner;

            var prefix = pathSegment.SubSegment(indexOfSeparator + 1, pathSegment.Length - indexOfSeparator - 2);
            return prefix[prefix.Length - 1] != '/' ? null : $"{prefix}{val}";
        }

        private static int FirstIndexOf(this StringSegment str, params char[] chars)
        {
            for (var inx = 0; inx < str.Length; inx++)
                foreach(var c in chars)
                    if (str[inx] == c)
                        return inx;

            return -1;
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
