using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Munin.Tree;

namespace Raven.Munin
{
    public class SecondaryIndex
    {
        private readonly string indexDef;
        private readonly IPersistentSource persistentSource;
        private readonly Func<JToken, IComparable> transform;

        public SecondaryIndex(Func<JToken, IComparable> transform, string indexDef, IPersistentSource persistentSource)
        {
            this.transform = transform;
            this.indexDef = indexDef;
            this.persistentSource = persistentSource;
        }

        private IBinarySearchTree<IComparable, IBinarySearchTree<JToken, JToken>> Index
        {
            get { return persistentSource.DictionariesStates[DictionaryId].SecondaryIndicesState[IndexId]; }
            set { persistentSource.DictionariesStates[DictionaryId].SecondaryIndicesState[IndexId] = value; }
        }

        public long Count
        {
            get { return Index.Count; }
        }

        public int DictionaryId { get; set; }

        public int IndexId { get; set; }

        public override string ToString()
        {
            return indexDef + " (" + Index.Count + ")";
        }

        public void Add(JToken key)
        {
            IComparable actualKey = transform(key);
            Index = Index.AddOrUpdate(actualKey, 
                new EmptyAVLTree<JToken, JToken>(JTokenComparer.Instance).Add(key,key),
                (comparable, tree) => tree.Add(key, key));
        }

        public void Remove(JToken key)
        {
            IComparable actualKey = transform(key);
            var result = Index.Search(actualKey);
            if (result.IsEmpty)
            {
                return;
            }
            bool removed;
            JToken _;
            var removedResult = result.Value.TryRemove(key, out removed, out _);
            if(removedResult.IsEmpty)
            {
                IBinarySearchTree<JToken, JToken> ignored;
                Index = Index.TryRemove(actualKey, out removed, out ignored);
            }
            else
            {
                Index = Index.AddOrUpdate(actualKey, removedResult, (comparable, tree) => removedResult);
            }
        }


        public IEnumerable<JToken> SkipFromEnd(int start)
        {
            return persistentSource.Read(_ => SkipFromEndInternal(start));
        }

        private IEnumerable<JToken> SkipFromEndInternal(int start)
        {
            return Index.Values.Skip(start).Select(item => item.Key);
        }

        public IEnumerable<JToken> SkipAfter(JToken key)
        {
            return Skip(key, i => i <= 0);
        }

        private IEnumerable<JToken> Skip(JToken key, Func<int, bool> shouldMoveToNext)
        {
            IComparable actualKey = transform(key);
            var recordingComparer = new RecordingComparer();
            Array.BinarySearch(Index.Keys.ToArray(), actualKey, recordingComparer);

            if (recordingComparer.LastComparedTo == null)
                yield break;

            var result = Index.Search(recordingComparer.LastComparedTo);

            if (shouldMoveToNext(recordingComparer.LastComparedTo.CompareTo(actualKey)))
                result = result.Right; // skip to the next higher value

            foreach (var item in result.Values)
            {
                yield return item.Key;
            }
        }

        public IEnumerable<JToken> SkipTo(JToken key)
        {
            return persistentSource.Read(_ => Skip(key, i => i < 0));
        }

        public JToken LastOrDefault()
        {
            return persistentSource.Read(_ =>
            {
                if (Index.Count == 0)
                    return null;
                return Index.LastOrDefault.LastOrDefault;
            });
        }

        public JToken FirstOrDefault()
        {
            return persistentSource.Read(_ =>
            {
                if (Index.Count == 0)
                    return null;
                return Index.FirstOrDefault.FirstOrDefault;
            });
        }

        public void Initialize(int dictionaryId, int indexId)
        {
            DictionaryId = dictionaryId;
            IndexId = indexId;
        }
    }
}