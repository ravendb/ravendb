using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public class BlittableJsonTraverser
    {
        public static BlittableJsonTraverser Default = new BlittableJsonTraverser();
        public static BlittableJsonTraverser FlatMapReduceResults = new BlittableJsonTraverser(new char[] { }); // map-reduce results have always a flat structure, let's ignore separators

        private const char PropertySeparator = '.';
        private const char CollectionSeparatorStartBracket = '[';
        private const char CollectionSeparatorEndBracket = ']';
        public static readonly char[] CollectionAndPropertySeparators = { CollectionSeparatorStartBracket, CollectionSeparatorEndBracket, PropertySeparator };

        public static readonly char[] PropertySeparators =
        {
            PropertySeparator
        };

        private readonly char[] _separators =
        {
            PropertySeparator,
            CollectionSeparatorStartBracket
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
                case CollectionSeparatorStartBracket:

                    var type = GetCollectionSeparatorType(path, propertySegmentLength);

                    switch (type)
                    {
                        case CollectionSeparatorType.BracketsOnly:
                            leftPath = string.Empty; // we are done with this path

                            if (reader is BlittableJsonReaderArray innerArray)
                            {
                                result = ReadArray(innerArray, leftPath);
                                return true;
                            }

                            result = null;
                            leftPath = path;
                            return false;
                        case CollectionSeparatorType.MultiBrackets:
                            leftPath = path.Subsegment(propertySegmentLength + 2);

                            if (reader is BlittableJsonReaderArray multiDimensionalArray)
                            {
                                result = ReadArray(multiDimensionalArray, leftPath);
                                leftPath = string.Empty; // we consume and handle internally the rest of it
                                return true;
                            }

                            result = reader;
                            return false;
                        case CollectionSeparatorType.BracketsAndProperty:
                            leftPath = path.Subsegment(propertySegmentLength + CollectionAndPropertySeparators.Length);

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
                            throw new NotSupportedException($"Invalid collection separator in a given path: {path}");
                    }
                    
                default:
                    throw new NotSupportedException($"Unhandled separator character: {path[propertySegmentLength]}");
            }
        }

        private enum CollectionSeparatorType
        {
            BracketsAndProperty,
            BracketsOnly,
            MultiBrackets,
            Invalid
        }

        private CollectionSeparatorType GetCollectionSeparatorType(StringSegment path, int propertySegmentLength)
        {
            if (path.Length < propertySegmentLength + CollectionAndPropertySeparators.Length)
            {
                if (path.Length <= propertySegmentLength + 1)
                    return CollectionSeparatorType.Invalid;

                if (path[propertySegmentLength + 1] != CollectionSeparatorEndBracket)
                    return CollectionSeparatorType.Invalid;

                return CollectionSeparatorType.BracketsOnly;
            }

            if (path[propertySegmentLength + 1] != CollectionSeparatorEndBracket)
                return CollectionSeparatorType.Invalid;

            if (path[propertySegmentLength + 2] != '.')
            {
                if (path[propertySegmentLength + 2] == CollectionSeparatorStartBracket)
                    return CollectionSeparatorType.MultiBrackets;

                return CollectionSeparatorType.Invalid;
            }

            return CollectionSeparatorType.BracketsAndProperty;
        }

        private StringSegment GetNextToken(StringSegment path, out int consumed)
        {
            int SkipQuote(char ch, int i)
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

            if (path.Length == 0)
            {
                consumed = 0;
                return path;
            }

            if (path[0] == '"' || path[0] == '\'')
            {
                consumed = SkipQuote(path[0], 1);
                return path.Subsegment(1, consumed - 2);
            }

            consumed = path.IndexOfAny(_separators, 0);
            if(consumed == -1)
                return path;
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
                if (item is BlittableJsonReaderObject arrayObject)
                {
                    if (BlittableJsonTraverserHelper.TryRead(this, arrayObject, pathSegment, out var result))
                    {
                        if (result is IEnumerable enumerable && result is string == false)
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
                    else
                    {
                        yield return null;
                    }
                }
                else
                {
                    if (item is BlittableJsonReaderArray arrayReader)
                    {
                        var type = GetCollectionSeparatorType(pathSegment, 0);

                        StringSegment subSegment;
                        switch (type)
                        {
                            case CollectionSeparatorType.BracketsAndProperty:
                                subSegment = pathSegment.Subsegment(CollectionAndPropertySeparators.Length);
                                break;
                            case CollectionSeparatorType.MultiBrackets:
                                subSegment = pathSegment.Subsegment(2);
                                break;
                            case CollectionSeparatorType.BracketsOnly:
                                subSegment = string.Empty;
                                break;
                            default:
                                throw new NotSupportedException($"Invalid collection separator in a given path: {pathSegment}");
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
