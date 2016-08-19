using System;
using System.Collections;
using System.Collections.Generic;

namespace Sparrow.Json
{
    public class BlittableJsonTraverser
    {
        public static BlittableJsonTraverser Default = new BlittableJsonTraverser();

        public const char PropertySeparator = '.';
        public const char CollectionSeparator = ',';

        public static readonly char[] PropertySeparators =
        {
            PropertySeparator
        };

        private readonly char[] _separators =
        {
            PropertySeparator,
            CollectionSeparator
        };

        public BlittableJsonTraverser(char[] nonDefaultSeparators = null)
        {
            if (nonDefaultSeparators != null)
                _separators = nonDefaultSeparators;
        }

        public object Read(BlittableJsonReaderObject docReader, StringSegment path)
        {
            object result;
            StringSegment leftPath;
            if (TryRead(docReader, path, out result, out leftPath) == false)
                throw new InvalidOperationException($"Invalid path: {path}.");

            return result;
        }

        public bool TryRead(BlittableJsonReaderObject docReader, StringSegment path, out object result, out StringSegment leftPath)
        {
            var indexOfFirstSeparator = path.IndexOfAny(_separators, 0);
            object reader;

            //if not found -> indexOfFirstSeparator == -1 -> take whole includePath as segment
            if (docReader.TryGetMember(path.SubSegment(0, indexOfFirstSeparator), out reader) == false)
            {
                leftPath = path;
                result = null;
                return false;
            }

            if (indexOfFirstSeparator == -1)
            {
                leftPath = path;
                result = reader;
                return true;
            }

            var pathSegment = path.SubSegment(indexOfFirstSeparator + 1);

            switch (path[indexOfFirstSeparator])
            {
                case PropertySeparator:
                    var subObject = reader as BlittableJsonReaderObject;
                    if (subObject != null)
                        return TryRead(subObject, pathSegment, out result, out leftPath);

                    leftPath = pathSegment;
                    result = reader;
                    return false;
                case CollectionSeparator:
                    var subArray = reader as BlittableJsonReaderArray;
                    if (subArray != null)
                    {
                        leftPath = pathSegment;
                        result = ReadArray(subArray, pathSegment);
                        return true;
                    }

                    throw new InvalidOperationException($"Invalid path. After the collection separator ('{CollectionSeparator}') {reader?.GetType()?.FullName ?? "null"}  object has been ancountered instead of {nameof(BlittableJsonReaderArray)}.");
                default:
                    throw new NotSupportedException($"Unhandled separator character: {path[indexOfFirstSeparator]}");
            }
        }

        private IEnumerable<object> ReadArray(BlittableJsonReaderArray array, StringSegment pathSegment)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < array.Length; i++)
            {
                var item = array[i];
                var arrayObject = item as BlittableJsonReaderObject;
                if (arrayObject != null)
                {
                    object result;
                    StringSegment leftPath;
                    if (TryRead(arrayObject, pathSegment, out result, out leftPath))
                    {
                        var enumerable = result as IEnumerable;

                        if (enumerable != null)
                        {
                            foreach (var nestedItem in enumerable)
                            {
                                yield return nestedItem;
                            }
                        }
                        else
                        {
                            yield return result;
                        }
                    }
                }
                else
                {
                    var arrayReader = item as BlittableJsonReaderArray;
                    if (arrayReader != null)
                    {
                        var indexOfFirstSeparatorInSubIndex = pathSegment.IndexOfAny(_separators, 0);
                        var subSegment = pathSegment.SubSegment(indexOfFirstSeparatorInSubIndex + 1);

                        foreach (var nestedItem in ReadArray(arrayReader, subSegment))
                        {
                            yield return nestedItem;
                        }
                    }
                    else
                    {
                        yield return item;
                    }
                }
            }
        }
    }
}