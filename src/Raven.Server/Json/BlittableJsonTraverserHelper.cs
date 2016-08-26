using System;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class BlittableJsonTraverserHelper
    {
        public static object Read(BlittableJsonTraverser blittableJsonTraverser, Document document, StringSegment path)
        {
            StringSegment leftPath;
            object value;
            if (blittableJsonTraverser.TryRead(document.Data, path, out value, out leftPath) == false)
            {
                value = TypeConverter.ConvertForIndexing(value);

                if (value == null)
                    return null;

                if (leftPath == "Length")
                {
                    var lazyStringValue = value as LazyStringValue;
                    if (lazyStringValue != null)
                        return lazyStringValue.Size;

                    var lazyCompressedStringValue = value as LazyCompressedStringValue;
                    if (lazyCompressedStringValue != null)
                        return lazyCompressedStringValue.UncompressedSize;

                    var array = value as BlittableJsonReaderArray;
                    if (array != null)
                        return array.Length;

                    return null;
                }

                if (leftPath == "Count")
                {
                    var array = value as BlittableJsonReaderArray;
                    if (array != null)
                        return array.Length;

                    return null;
                }

                if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                {
                    int indexOfPropertySeparator;
                    do
                    {
                        indexOfPropertySeparator = leftPath.IndexOfAny(BlittableJsonTraverser.PropertySeparators, 0);
                        if (indexOfPropertySeparator != -1)
                            leftPath = leftPath.SubSegment(0, indexOfPropertySeparator);

                        var accessor = TypeConverter.GetPropertyAccessor(value);
                        value = accessor.GetValue(leftPath, value);

                        if (value == null)
                            return null;

                    } while (indexOfPropertySeparator != -1);

                    return value;
                }

                throw new InvalidOperationException($"Could not extract {path} from {document.Key}.");
            }

            return TypeConverter.ConvertForIndexing(value);
        }
    }
}