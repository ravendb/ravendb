using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Client.Indexing;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items); 

    public abstract class StaticIndexBase
    {
        public readonly Dictionary<string, IndexingFunc[]> Maps = new Dictionary<string, IndexingFunc[]>(StringComparer.OrdinalIgnoreCase);

        public IndexDefinition Definition;

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
    }
}