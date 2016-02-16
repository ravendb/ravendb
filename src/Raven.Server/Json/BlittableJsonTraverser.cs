using System;
using System.Collections.Generic;
using Raven.Server.Routing;

namespace Raven.Server.Json
{
    public class BlittableJsonTraverser // TODO arek - based on IncludeUtilTests - maybe it should be used there to get ids of included docs
    {
        private const char PropertySeparator = '.';
        private const char CollectionSeparator = ',';

        private readonly char[] _separators;

        public BlittableJsonTraverser(char[] separators = null)
        {
            _separators = separators ?? new []{ PropertySeparator, CollectionSeparator };
        }

        public object Read(BlittableJsonReaderObject docReader, StringSegment path)
        {
            var indexOfFirstSeparator = path.IndexOfAny(_separators, 0);
            object reader;

            //if not found -> indexOfFirstSeparator == -1 -> take whole includePath as segment
            if (docReader.TryGetMember(path.SubSegment(0, indexOfFirstSeparator), out reader) == false || indexOfFirstSeparator == -1)
                return reader;

            var pathSegment = path.SubSegment(indexOfFirstSeparator + 1);

            switch (path[indexOfFirstSeparator])
            {
                case PropertySeparator:
                    var subObject = reader as BlittableJsonReaderObject;
                    if (subObject != null)
                        return Read(subObject, pathSegment);
                    
                    throw new InvalidOperationException($"Invalid path. After the property separator ('{PropertySeparator}') {reader.GetType().FullName} object has been ancountered instead of {nameof(BlittableJsonReaderObject)}." );
                case CollectionSeparator:
                    var subArray = reader as BlittableJsonReaderArray;
                    if (subArray != null)
                        return ReadArray(subArray, pathSegment);

                    throw new InvalidOperationException($"Invalid path. After the collection separator ('{CollectionSeparator}') {reader.GetType().FullName} object has been ancountered instead of {nameof(BlittableJsonReaderArray)}.");
                //case '(':
                //    if (includePath[includePath.Length - 1] != ')') //precaution
                //        return;

                //    var idWithPrefix = HandlePrefix(docReader, includePath, indexOfFirstSeparator);
                //    if (idWithPrefix != null)
                //        includedIds.Add(idWithPrefix);
                //    break;
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
                    yield return Read(arrayObject, pathSegment);
                }
                else
                {
                    var arrayReader = item as BlittableJsonReaderArray;
                    if (arrayReader != null)
                    {
                        var indexOfFirstSeparatorInSubIndex = pathSegment.IndexOfAny(_separators, 0);
                        var subSegment = pathSegment.SubSegment(indexOfFirstSeparatorInSubIndex + 1);

                        yield return ReadArray(arrayReader, subSegment); //TODO arek
                        
                        continue;
                    }

                    yield return item;
                }
            }
        }
    }
}