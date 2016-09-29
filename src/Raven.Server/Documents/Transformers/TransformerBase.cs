using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerBase
    {
        public IndexingFunc TransformResults { get; set; }

        public bool HasGroupBy { get; set; }

        public bool HasLoadDocument { get; set; }

        public bool HasTransformWith { get; set; }

        public bool HasInclude { get; set; }

        public string Source { get; set; }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
        }

        public dynamic LoadDocument<TIGnored>(object keyOrEnumerable, string collectionName)
        {
            return LoadDocument(keyOrEnumerable, collectionName);
        }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("Transformation scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable == null || keyOrEnumerable is DynamicNullObject)
                return DynamicNullObject.Null;

            var keyLazy = keyOrEnumerable as LazyStringValue;
            if (keyLazy != null)
                return CurrentTransformationScope.Current.LoadDocument(keyLazy, null);

            var keyString = keyOrEnumerable as string;
            if (keyString != null)
                return CurrentTransformationScope.Current.LoadDocument(null, keyString);

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
                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public dynamic Include(object key)
        {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("Transformation scope was not initialized.");

            return CurrentTransformationScope.Current.Include(key);
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

        public IEnumerable<dynamic> TransformWith(IEnumerable<string> transformers, dynamic maybeItems)
        {
            return Enumerable.Aggregate(transformers, maybeItems, (Func<dynamic, string, dynamic>)((items, transformer) => TransformWith(transformer, items)));
        }

        public IEnumerable<dynamic> TransformWith(string transformer, dynamic maybeItems)
        {
            if (CurrentTransformationScope.Current == null)
                throw new InvalidOperationException("TransformWith was accessed without CurrentTransformationScope.Current being set");

            return CurrentTransformationScope.Current.TransformWith(transformer, maybeItems);
        }
    }
}