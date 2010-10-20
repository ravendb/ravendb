using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Munin.Tree;

namespace Raven.Munin
{
    public class PersistentDictionaryState
    {
        public IBinarySearchTree<JToken, PositionInFile> KeyToFilePositionInFiles { get; set; }

        public List<SecondaryIndex> SecondaryIndices { get; set; }

        public ICompererAndEquality<JToken> Comparer { get; set; }

        public PersistentDictionaryState(ICompererAndEquality<JToken> comparer)
        {
            Comparer = comparer;
            SecondaryIndices = new List<SecondaryIndex>();
            KeyToFilePositionInFiles = new EmptyAVLTree<JToken, PositionInFile>(Comparer);
        }
    }
}