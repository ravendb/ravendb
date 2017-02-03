using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Sparrow.Json;

namespace Raven.NewClient.Json.Utilities
{
    public static class TransformerHelpers
    {

        public static IEnumerable<T> ParseResults<T>(InMemoryDocumentSessionOperations session, GetDocumentResult transformedResult)
        {
            foreach (BlittableJsonReaderObject result in transformedResult.Results)
            {
                if (result == null)
                {
                    yield return default(T);
                    continue;
                }

                BlittableJsonReaderArray values;
                if (result.TryGet(Constants.Json.Fields.Values, out values) == false)
                    throw new InvalidOperationException("Transformed document must have a $values property");

                foreach (var value in ParseValuesFromBlittableArray<T>(session, values))
                    yield return value;
            }
        }

        public static IEnumerable<T> ParseValuesFromBlittableArray<T>(InMemoryDocumentSessionOperations session, BlittableJsonReaderArray blittableArray)
        {
            for (var i = 0; i < blittableArray.Length; i++)
            {
                var blittableJsonToken = blittableArray.GetValueTokenTupleByIndex(i).Item2;

                switch (blittableJsonToken)
                {
                    case BlittableJsonToken.StartArray:
                        foreach (var inner in ParseValuesFromBlittableArray<T>(session, blittableArray[i] as BlittableJsonReaderArray))
                        {
                            yield return inner;
                        }
                        break;
                    case BlittableJsonToken.StartObject:
                        yield return (T)session.DeserializeFromTransformer(typeof(T), null, blittableArray[i] as BlittableJsonReaderObject);
                        break;
                    case BlittableJsonToken.String:
                        var lazyString = blittableArray[i] as LazyStringValue;
                        if (lazyString != null)
                            yield return (T)(object)lazyString.ToString();
                        break;
                    case BlittableJsonToken.CompressedString:
                        var lazyCompressedString = blittableArray[i] as LazyCompressedStringValue;
                        if (lazyCompressedString != null)
                            yield return (T)(object)lazyCompressedString.ToString();
                        break;
                    default:
                        // TODO, check if other types need special handling as well
                        yield return (T)blittableArray[i];
                        break;
                }
            }
        }

        public static T[] ParseResultsArray<T>(InMemoryDocumentSessionOperations session, GetDocumentResult transformedResult)
        {
            return transformedResult.Results.Select(x =>
            {
                if (x == null)
                    return null;

                BlittableJsonReaderArray values;
                if (((BlittableJsonReaderObject)x).TryGet(Constants.Json.Fields.Values, out values) == false)
                    throw new InvalidOperationException("Transformed document must have a $values property");

                var elementType = typeof(T).GetElementType();

                var array = values.Select(value => session.DeserializeFromTransformer(elementType, null, value as BlittableJsonReaderObject)).ToArray();
                var newArray = Array.CreateInstance(elementType, array.Length);
                Array.Copy(array, newArray, array.Length);
                return newArray;
            })
            .Cast<T>()
            .ToArray();
        }
    }
}
