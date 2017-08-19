using System;
using System.Collections;
using System.Collections.Generic;
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
            var propertySegment = path.Subsegment(0, indexOfFirstSeparator);
            if (docReader.TryGetMember(propertySegment, out reader) == false)
            {
                leftPath = path;
                result = null;
                return false;
            }

            if (indexOfFirstSeparator == -1)
            {
                leftPath = string.Empty;// we read it all
                result = reader;
                return true;
            }
            

            switch (path[indexOfFirstSeparator])
            {
                case PropertySeparator:
                    var pathSegment = path.Subsegment(indexOfFirstSeparator + 1);

                    var propertyInnerObject = reader as BlittableJsonReaderObject;
                    if (propertyInnerObject != null)
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
                    if (path.Length <= indexOfFirstSeparator + 2 ||
                        path[indexOfFirstSeparator + 1] != ']' ||
                        path[indexOfFirstSeparator + 2] != '.')
                    {
                        if (indexOfFirstSeparator + 1 < path.Length &&
                            path[indexOfFirstSeparator + 1] == ']')
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
                    leftPath = path.Subsegment(indexOfFirstSeparator + CollectionSeparator.Length);

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
                    throw new NotSupportedException($"Unhandled separator character: {path[indexOfFirstSeparator]}");
            }
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
