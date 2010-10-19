using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class PersistentDictionaryState
    {
        public ConcurrentDictionary<JToken, PositionInFile> KeyToFilePositionInFiles { get; set; }

        public List<SecondaryIndex> SecondaryIndices { get; set; }

        public IEqualityComparer<JToken> Comparer { get; set; }


        public override string ToString()
        {
            return KeyToFilePositionInFiles.Count.ToString();
        }

        public PersistentDictionaryState(IEqualityComparer<JToken> comparer)
        {
            Comparer = comparer;
            SecondaryIndices = new List<SecondaryIndex>();
            KeyToFilePositionInFiles = new ConcurrentDictionary<JToken, PositionInFile>(Comparer);
        }
    }
}