using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items); 

    public abstract class StaticIndexBase
    {
        public readonly Dictionary<string, IndexingFunc[]> Maps = new Dictionary<string, IndexingFunc[]>(StringComparer.OrdinalIgnoreCase);

        public string Source;

        public void AddMap(string collection, IndexingFunc map)
        {
            int len = 0;
            IndexingFunc[] mapsArray;
            if (Maps.TryGetValue(collection, out mapsArray))
            {
                len = mapsArray.Length;
            }
            Array.Resize(ref mapsArray, len + 1);
            mapsArray[len] = map;
            Maps[collection] = mapsArray;
        }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            var keyLazy = keyOrEnumerable as LazyStringValue;
            if (keyLazy != null)
                return CurrentIndexingScope.Current.LoadDocument(keyLazy, null, collectionName);

            var keyString = keyOrEnumerable as string;
            if (keyString != null)
                return CurrentIndexingScope.Current.LoadDocument(null, keyString, collectionName);

            //var enumerable = keyOrEnumerable as IEnumerable;
            //if (enumerable != null)
            //{
            //    var enumerator = enumerable.GetEnumerator();
            //    using (enumerable as IDisposable)
            //    {
            //        var items = new List<dynamic>();
            //        while (enumerator.MoveNext())
            //        {
            //            items.Add(LoadDocument(enumerator.Current, collectionName));
            //        }
            //        return null;
            //    }
            //}

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public IndexingFunc Reduce;

        public void SetReduce(string collection, IndexingFunc reduce)
        {
            Reduce = reduce;
        }
    }
}