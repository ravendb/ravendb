using System;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class BlittableJsonTraverserHelper
    {
        public static bool TryRead(BlittableJsonTraverser blittableJsonTraverser, Document document, StringSegment path, out object value)
        {
            StringSegment leftPath;
            if (blittableJsonTraverser.TryRead(document.Data, path, out value, out leftPath) == false)
            {
                value = TypeConverter.ConvertForIndexing(value);

                if (value == null)
                    return false;

                if (leftPath == "Length")
                {
                    var lazyStringValue = value as LazyStringValue;
                    if (lazyStringValue != null)
                    {
                        value = lazyStringValue.Size;
                        return true;
                    }


                    var lazyCompressedStringValue = value as LazyCompressedStringValue;
                    if (lazyCompressedStringValue != null)
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

                    value = null;
                    return false;
                }

                if (leftPath == "Count")
                {
                    var array = value as BlittableJsonReaderArray;
                    if (array != null)
                    {
                        value = array.Length;
                        return true;
                    }

                    value = null;
                    return false;
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
                            return false;

                    } while (indexOfPropertySeparator != -1);

                    return true;
                }

                throw new InvalidOperationException($"Could not extract {path} from {document.Key}.");
            }

            value = TypeConverter.ConvertForIndexing(value);
            return true;
        }
    }
}