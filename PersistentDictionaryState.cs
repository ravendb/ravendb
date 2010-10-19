using System.Collections.Concurrent;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class PersistentDictionaryState
    {
        public ConcurrentDictionary<JToken, PositionInFile> KeyToFilePositionInFiles { get; set; }

        public List<SecondaryIndex> SecondaryIndices { get; set; }


        public PersistentDictionaryState()
        {
            SecondaryIndices = new List<SecondaryIndex>();
        }
    }
}