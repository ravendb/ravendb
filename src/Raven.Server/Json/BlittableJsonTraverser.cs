using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public class BlittableJsonTraverser
    {
        public static BlittableJsonTraverser Default = new BlittableJsonTraverser();
        public static BlittableJsonTraverser FlatMapReduceResults = new BlittableJsonTraverser(new char[] { }); // map-reduce results have always a flat structure, let's ignore separators

        private const char PropertySeparator = '.';
        private const char CollectionSeparatorStart = '[';
        public static readonly char[] CollectionSeparator = { CollectionSeparatorStart, ']', PropertySeparator };

        public static readonly char[] PropertySeparators =
        {
            PropertySeparator
        };

        private readonly char[] _separators =
        {
            PropertySeparator,
            CollectionSeparatorStart
        };

        private BlittableJsonTraverser(char[] nonDefaultSeparators = null)
        {
            if (nonDefaultSeparators != null)
                _separators = nonDefaultSeparators;
        }

        public object Read(BlittableJsonReaderObject docReader, StringSegment path)
        {
            if (TryRead(docReader, path, out var result, out _) == false)
                throw new InvalidOperationException($"Invalid path: {path}.");

            return result;
        }

        public bool TryRead(BlittableJsonReaderObject docReader, StringSegment path, out object result, out StringSegment leftPath)
        {
            var propertySegment = GetNextToken(path, out int propertySegmentLength);
            if (docReader.TryGetMember(propertySegment, out var reader) == false)
            {
                leftPath = path;
                result = null;
                return false;
            }

            if (propertySegmentLength == path.Length || propertySegmentLength == -1)
            {
                leftPath = string.Empty;// we read it all
                result = reader;
                return true;
            }
            

            switch (path[propertySegmentLength])
            {
                case PropertySeparator:
                    var pathSegment = path.Subsegment(propertySegmentLength + 1);

                    if (reader is BlittableJsonReaderObject propertyInnerObject)
                    {
                        if (TryRead(propertyInnerObject, pathSegment, out result, out leftPath))
                            return true;

                        if (result == null)
                            result = reader;

                        return false;
                    }

                    leftPath = pathSegment;
                    result = reader;
                    return false;
                case CollectionSeparatorStart:
                    if (path.Length <= propertySegmentLength + 2 ||
                        path[propertySegmentLength + 1] != ']' ||
                        path[propertySegmentLength + 2] != '.')
                    {
                        if (propertySegmentLength + 1 < path.Length &&
                            path[propertySegmentLength + 1] == ']')
                        {
                            if (reader is BlittableJsonReaderArray innerArray)
                            {
                                leftPath = string.Empty; // we are done with this path
                                result = ReadArray(innerArray, leftPath);
                                return true;
                            }
                        }
                        result = null;
                        leftPath = path;
                        return false;
                    }
                    leftPath = path.Subsegment(propertySegmentLength + CollectionSeparator.Length);

                    if (reader is BlittableJsonReaderArray collectionInnerArray)
                    {
                        result = ReadArray(collectionInnerArray, leftPath);
                        leftPath = string.Empty; // we consume and handle internally the rest of it
                        return true;
                    }

                    if (reader is BlittableJsonReaderObject nested)
                    {
                        return ReadNestedObjects(nested, leftPath, out result, out leftPath);

                    }
                    result = reader;
                    return false;
                default:
                    throw new NotSupportedException($"Unhandled separator character: {path[propertySegmentLength]}");
            }
        }

        private StringSegment GetNextToken(StringSegment path, out int consumed)
        {
            int SkipQoute(char ch, int i)
            {
                for (int j = i; j < path.Length; j++)
                {
                    if (path[j] == '\'')
                    {
                        j++;// escape next chart
                        continue;
                    }
                    if (path[j] == ch)
                        return j;
                }
                return path.Length;
            }

            if (path[0] == '"' || path[0] == '\'')
            {
                consumed = SkipQoute(path[0], 1);
                return path.Subsegment(1, consumed - 2);
            }

            consumed = path.IndexOfAny(_separators, 0);
            return path.Subsegment(0, consumed);
        }

        private bool ReadNestedObjects(BlittableJsonReaderObject nested, StringSegment path, out object result, out StringSegment leftPath)
        {
            var propCount = nested.Count;
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            List<object> results = null;
            leftPath = path; // note that we assume identical sturcutre of all values in this document
            void AddItemToResults(object i)
            {
                if (results == null)
                    results = new List<object>();
                results.Add(i);
            }
            for (int i = 0; i < propCount; i++)
            {
                nested.GetPropertyByIndex(i, ref prop);
                if (prop.Value is BlittableJsonReaderObject nestedVal)
                {
                    if (TryRead(nestedVal, path, out var item, out leftPath))
                    {
                        AddItemToResults(item);
                    }
                    else
                    {
                        item = item ?? nested;
                        if (BlittableJsonTraverserHelper.TryReadComputedProperties(this, leftPath, ref item))
                        {
                            leftPath = string.Empty;// consumed entirely
                            AddItemToResults(item);
                        }
                    }
                }
            }
            result = results ?? (object)nested;
            return results != null;
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
                    if (BlittableJsonTraverserHelper.TryRead(this, arrayObject, pathSegment, out result))
                    {
                        var enumerable = result as IEnumerable;

                        if (enumerable != null && result is string == false)
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

                        string subSegment;

                        switch (pathSegment[indexOfFirstSeparatorInSubIndex])
                        {
                            case PropertySeparator:
                                subSegment = pathSegment.Subsegment(indexOfFirstSeparatorInSubIndex + 1);
                                break;
                            case CollectionSeparatorStart:
                                subSegment = pathSegment.Subsegment(indexOfFirstSeparatorInSubIndex + 3);
                                break;
                            default:
                                throw new NotSupportedException($"Unhandled separator character: {pathSegment[indexOfFirstSeparatorInSubIndex]}");
                        }

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
