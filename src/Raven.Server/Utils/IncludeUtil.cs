using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Json;
using Raven.Server.Routing;

namespace Raven.Server.Utils
{
    public static class IncludeUtil
    {	   
        public static IEnumerable<string> GetDocIdFromInclude(BlittableJsonReaderObject docReader, string includePath)
        {
            var indexOfFirstSeparator = includePath.IndexOfAny(new []{'.',',','('}, 0);
            object reader;
            docReader.TryGetMember(new StringSegment(includePath, 0, indexOfFirstSeparator), out reader);

            var pathSegment = new StringSegment(includePath, indexOfFirstSeparator + 1);
            switch (indexOfFirstSeparator != -1 ? includePath[indexOfFirstSeparator] : ' ')
            {
                case '.':
                    foreach (var id in HandleObjectProperty(reader, pathSegment))
                        yield return id;
                    break;
                case ',':
                    foreach (var id in HandleArrayProperty(reader, pathSegment))
                        yield return id;
                    break;
                case '(':
                    if (includePath[includePath.Length - 1] != ')') //precaution
                        yield break;

                    yield return HandlePrefix(docReader, new StringSegment(includePath, 0), indexOfFirstSeparator);
                    break;
                default:
                    object value;
                    if (docReader.TryGetMember(includePath, out value))
                        yield return BlittableValueToString(value);
                    break;
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

        private static IEnumerable<string> HandleArrayProperty(object reader, StringSegment pathSegment)
        {
            var readerArray = reader as BlittableJsonReaderArray;
            if (readerArray != null)
            {
                foreach (var id in GetIDsFromIncludePath(readerArray, pathSegment))
                    yield return id;
            }
        }

        private static IEnumerable<string> HandleObjectProperty(object reader, StringSegment pathSegment)
        {
            var readerObject = reader as BlittableJsonReaderObject;
            if (readerObject != null)
            {
                foreach (var id in GetIDsFromIncludePath(readerObject, pathSegment))
                    yield return id;
            }
        }

        private static IEnumerable<string> GetIDsFromIncludePath(BlittableJsonReaderObject docReader,
            StringSegment includePathSegment)
        {
            var index = includePathSegment.FirstIndexOf('.',',','(');

            if (index == -1) //whats left is a primitive value property
            {
                object val;
                var props = docReader.GetPropertyNames();
                if (docReader.TryGetMember(includePathSegment, out val))
                    yield return BlittableValueToString(val);
                yield break;
            }

            var propertyName = includePathSegment.SubSegment(0, index);
            object property;
            if (!docReader.TryGetMember(propertyName, out property))
                yield break;

            switch (includePathSegment[index])
            {
                case '.':
                    var objProperty = property as BlittableJsonReaderObject;
                    if (objProperty != null)
                        foreach (var res in GetIDsFromIncludePath(objProperty, includePathSegment.SubSegment(index + 1)))
                            yield return res;
                    break;
                case ',':
                    var arrProperty = property as BlittableJsonReaderArray;
                    if (arrProperty != null)
                        foreach (var res in GetIDsFromIncludePath(arrProperty, includePathSegment.SubSegment(index + 1)))
                            yield return res;
                    break;
                case '(':
                    //prefix subsegment without parentesis
                    yield return $"{includePathSegment.SubSegment(index + 1, includePathSegment.Length - index - 2)}{property}";
                    break;
            }


        }

        private static IEnumerable<string> GetIDsFromIncludePath(BlittableJsonReaderArray docReader,
                  StringSegment includePathSegment)
        {	    
            var index = includePathSegment.FirstIndexOf('.', '(');
            switch ((index == -1) ? ' ' : includePathSegment[index])
            {
                case '.':
                    foreach (var val in docReader)
                    {
                        var objReader = val as BlittableJsonReaderObject;
                        object property;
                        if (objReader != null && 
                            objReader.TryGetMember(includePathSegment.SubSegment(0,index),out property))
                        {
                            var subReader = property as BlittableJsonReaderObject;
                            if(subReader != null)
                                foreach (var id in GetIDsFromIncludePath(subReader, includePathSegment.SubSegment(index + 1)))
                                yield return id;
                        }
                        else
                        {
                            yield return BlittableValueToString(val);
                        }
                    }
                    break;
                case '(':
                    foreach (var val in docReader)
                    {
                        var property = BlittableValueToString(val);
                        yield return $"{includePathSegment.SubSegment(index + 1, includePathSegment.Length - index - 2)}{property}";
                    }
                    break;
                default:
                    foreach (var val in docReader)
                    {
                        var objReader = val as BlittableJsonReaderObject;
                        object property;
                        if (objReader != null && objReader.TryGetMember(includePathSegment.SubSegment(index + 1), out property))
                            yield return BlittableValueToString(property);
                        else
                        {
                            var returnValue = BlittableValueToString(val);
                            if (returnValue != null)
                                yield return returnValue;
                        }
                    }
                    break;
            }

            yield break;
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

            var lazyDoubleVal = value as LazyDoubleValue;
            if (lazyDoubleVal != null)
                return lazyDoubleVal.Inner.ToString();

            var convertible = value as IConvertible;
            return convertible?.ToString(CultureInfo.InvariantCulture);
        }

    }
}
