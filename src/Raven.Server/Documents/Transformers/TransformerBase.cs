using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerBase
    {
        public IndexingFunc TransformResults { get; set; }

        public string Source { get; set; }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("Transformation scope was not initialized. Key: " + keyOrEnumerable);

            var keyLazy = keyOrEnumerable as LazyStringValue;
            if (keyLazy != null)
                return CurrentTransformationScope.Current.LoadDocument(keyLazy, null, collectionName);

            var keyString = keyOrEnumerable as string;
            if (keyString != null)
                return CurrentTransformationScope.Current.LoadDocument(null, keyString, collectionName);

            var enumerable = keyOrEnumerable as IEnumerable;
            if (enumerable != null)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadDocument(enumerator.Current, collectionName));
                    }
                    return new DynamicJsonArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public dynamic Parameter(string key)
        {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("Transformation scope was not initialized.");

            return CurrentTransformationScope.Current.Parameter(key);
        }

        public dynamic ParameterOrDefault(string key, object val)
        {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("Transformation scope was not initialized.");

            return CurrentTransformationScope.Current.ParameterOrDefault(key, val);
        }
    }
}