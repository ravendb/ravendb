using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Server.Documents;
using Sparrow;
using Sparrow.Json;
using TypeConverter = Raven.Server.Utils.TypeConverter;

namespace Raven.Server.Json
{
    public static class BlittableJsonTraverserHelper
    {
        private const string LastModifiedPath = Constants.Documents.Metadata.Key + "." + Constants.Documents.Metadata.LastModified;

        public static bool TryRead(
            BlittableJsonTraverser blittableJsonTraverser,
            Document document,
            StringSegment path,
            out object value)
        {
            if (TryRead(blittableJsonTraverser, document.Data, path, out value))
                return true;

            if (path == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
            {
                value = document.Id;
                return true;
            }

            if (path == LastModifiedPath)
            {
                value = document.LastModified;
                return true;
            }

            value = null;
            return false;
        }

        public static bool TryRead(
            BlittableJsonTraverser blittableJsonTraverser,
            BlittableJsonReaderObject document,
            StringSegment path,
            out object value)
        {
            if (blittableJsonTraverser.TryRead(document, path, out value, out StringSegment leftPath) &&
                leftPath.Length == 0)
            {
                value = TypeConverter.ConvertForIndexing(value);
                return true;
            }

            if (value == null)
                return false;

            return TryReadComputedProperties(blittableJsonTraverser, leftPath, ref value);
        }


        public static bool TryReadComputedProperties(BlittableJsonTraverser blittableJsonTraverser, StringSegment leftPath, ref object value)
        {
            value = TypeConverter.ConvertForIndexing(value);

            if (leftPath == "Length" || leftPath == "length" ||
                leftPath == "Count" || leftPath == "count")
            {
                if (value is LazyStringValue lazyStringValue)
                {
                    value = lazyStringValue.Size;
                    return true;
                }

                if (value is string s)
                {
                    value = s.Length;
                    return true;
                }

                if (value is LazyCompressedStringValue lazyCompressedStringValue)
                {
                    value = lazyCompressedStringValue.UncompressedSize;
                    return true;
                }

                var array = value as BlittableJsonReaderArray;
                if (array != null)
                {
                    value = array.Length;
                    return true;
                }

                if (value is BlittableJsonReaderObject json)
                {
                    value = json.Count;
                    return true;
                }

                if (value is Array a)
                {
                    value = a.Length;
                    return true;
                }

                if (value is List<object> l)
                {
                    value = l.Count;
                    return true;
                }

                value = null;
                return false;
            }

            if (value is BlittableJsonReaderObject obj) // dictionary key e.g. .Where(x => x.Events.Any(y => y.Key.In(dates)))
            {
                var isKey = leftPath == "Key";
                if (isKey || leftPath == "Value")
                {
                    var index = 0;
                    var property = new BlittableJsonReaderObject.PropertyDetails();
                    var values = new object[obj.Count];

                    for (int i = 0; i < obj.Count; i++)
                    {
                        obj.GetPropertyByIndex(i, ref property);
                        var val = isKey ? property.Name : property.Value;
                        values[index++] = TypeConverter.ConvertForIndexing(val);
                    }

                    value = values;
                    return true;
                }
                if (TryRead(blittableJsonTraverser, obj, leftPath, out value))
                    return true;
            }

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
            {
                int indexOfPropertySeparator;
                do
                {
                    indexOfPropertySeparator = leftPath.IndexOfAny(BlittableJsonTraverser.PropertySeparators, 0);
                    if (indexOfPropertySeparator != -1)
                        leftPath = leftPath.Subsegment(0, indexOfPropertySeparator);

                    var accessor = TypeConverter.GetPropertyAccessor(value);
                    value = accessor.GetValue(leftPath.Value, value);

                    if (value == null)
                        return false;
                } while (indexOfPropertySeparator != -1);

                return true;
            }

            if (value is string == false &&
                value is IEnumerable items)
            {
                value = ReadNestedComputed(blittableJsonTraverser, items, leftPath);
                return true;
            }

            value = null;
            return false;
        }

        private static IEnumerable<object> ReadNestedComputed(BlittableJsonTraverser blittableJsonTraverser, IEnumerable items, StringSegment leftPath)
        {
            foreach (var item in items)
            {
                var current = item;
                if (TryReadComputedProperties(blittableJsonTraverser, leftPath, ref current))
                    yield return current;
            }
        }
    }
}
