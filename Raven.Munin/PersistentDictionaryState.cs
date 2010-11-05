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

        public List<IBinarySearchTree<IComparable, IBinarySearchTree<JToken, JToken>>> SecondaryIndicesState { get; set; }

        public ICompererAndEquality<JToken> Comparer { get; set; }

        public PersistentDictionaryState(ICompererAndEquality<JToken> comparer)
        {
            Comparer = comparer;
            SecondaryIndicesState = new List<IBinarySearchTree<IComparable, IBinarySearchTree<JToken, JToken>>>();
            KeyToFilePositionInFiles = new EmptyAVLTree<JToken, PositionInFile>(Comparer, JTokenCloner.Clone, file => new PositionInFile
            {
                Key = JTokenCloner.Clone(file.Key),
                Position = file.Position,
                Size = file.Size
            });
        }
    }
}