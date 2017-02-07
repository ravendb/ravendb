using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Extensions;
using Sparrow.Json;

namespace Raven.NewClient.Json.Utilities
{
    internal static class TransformerHelper
    {
        public static Dictionary<string, T> ParseResultsForLoadOperation<T>(InMemoryDocumentSessionOperations session, GetDocumentResult transformedResult)
        {
            var result = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            var resultType = typeof(T);
            var allowMultiple = resultType.IsArray;

            for (var i = 0; i < transformedResult.Results.Length; i++)
            {
                var item = transformedResult.Results[i] as BlittableJsonReaderObject;
                if (item == null)
                    continue;

                var metadata = item.GetMetadata();
                var id = metadata.GetId();

                result[id] = ParseSingleResult<T>(id, item, session, allowMultiple, resultType);
            }

            return result;
        }

        public static Dictionary<string, T> ParseResultsForQueryOperation<T>(InMemoryDocumentSessionOperations session, QueryResultBase transformedResult)
        {
            var result = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            var resultType = typeof(T);
            var allowMultiple = resultType.IsArray;

            for (var i = 0; i < transformedResult.Results.Length; i++)
            {
                var item = transformedResult.Results[i] as BlittableJsonReaderObject;
                if (item == null)
                    continue;

                var metadata = item.GetMetadata();
                var id = metadata.GetId();

                result[id] = ParseSingleResult<T>(id, item, session, allowMultiple, resultType);
            }

            return result;
        }

        public static IEnumerable<T> ParseResultsForStreamOperation<T>(InMemoryDocumentSessionOperations session, BlittableJsonReaderArray array)
        {
            return ParseValuesFromBlittableArray(typeof(T), session, array)
                .Cast<T>();
        }

        private static T ParseSingleResult<T>(string id, BlittableJsonReaderObject item, InMemoryDocumentSessionOperations session, bool allowMultiple, Type resultType)
        {
            if (item == null)
                return default(T);

#if DEBUG
            var metadata = item.GetMetadata();
            var resultId = metadata.GetId();

            if (string.Equals(id, resultId, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException("Ids do not match.");
#endif

            BlittableJsonReaderArray values;
            if (item.TryGet(Constants.Json.Fields.Values, out values) == false)
                throw new InvalidOperationException("Transformed document must have a $values property");

            if (allowMultiple == false && values.Length > 1)
                throw new InvalidOperationException(
                    string.Format("An operation was attempted with transformer and more than one item was returned per entity - please use {0}[] as the projection type instead of {0}", typeof(T).Name));

            if (allowMultiple)
            {
                var elementType = typeof(T).GetElementType();

                var array = ParseValuesFromBlittableArray(elementType, session, values).ToArray();
                var newArray = Array.CreateInstance(elementType, array.Length);
                Array.Copy(array, newArray, array.Length);

                return (T)(object)newArray;
            }

            return ParseValuesFromBlittableArray(resultType, session, values)
                .Cast<T>()
                .FirstOrDefault();
        }

        private static IEnumerable<object> ParseValuesFromBlittableArray(Type type, InMemoryDocumentSessionOperations session, BlittableJsonReaderArray array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                var token = array.GetValueTokenTupleByIndex(i).Item2;

                switch (token)
                {
                    case BlittableJsonToken.StartArray:
                        foreach (var inner in ParseValuesFromBlittableArray(type, session, array[i] as BlittableJsonReaderArray))
                            yield return inner;
                        break;
                    case BlittableJsonToken.StartObject:
                        yield return session.DeserializeFromTransformer(type, null, array[i] as BlittableJsonReaderObject);
                        break;
                    case BlittableJsonToken.String:
                        var lazyString = array[i] as LazyStringValue;
                        if (lazyString != null)
                            yield return lazyString.ToString();
                        break;
                    case BlittableJsonToken.CompressedString:
                        var lazyCompressedString = array[i] as LazyCompressedStringValue;
                        if (lazyCompressedString != null)
                            yield return lazyCompressedString.ToString();
                        break;
                    default:
                        // TODO, check if other types need special handling as well
                        yield return array[i];
                        break;
                }
            }
        }
    }
}
