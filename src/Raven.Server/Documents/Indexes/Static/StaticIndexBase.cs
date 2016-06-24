using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items); 

    public abstract class StaticIndexBase
    {
        public readonly Dictionary<string, IndexingFunc> Maps = new Dictionary<string, IndexingFunc>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, HashSet<string>> ReferencedCollections = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        public string Source;

        public void AddMap(string collection, IndexingFunc map)
        {
            Maps[collection] = map;
        }

        public void AddReferencedCollection(string collection, string referencedCollection)
        {
            HashSet<string> set;
            if (ReferencedCollections.TryGetValue(collection, out set) == false)
                ReferencedCollections[collection] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            set.Add(referencedCollection);
        }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
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
            //        return new DynamicList(items);
            //    }
            //}

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public IndexingFunc Reduce;

        public void SetReduce(IndexingFunc reduce, string[] groupByFields)
        {
            Reduce = reduce;
            GroupByFields = groupByFields;
        }

        public string[] GroupByFields;
    }
}