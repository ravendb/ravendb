using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class SecondaryIndex
    {
        private readonly IComparer<JToken> comparer;
        private readonly string indexDef;
        private readonly IPersistentSource persistentSource;
        private readonly SortedList<JToken, SortedSet<JToken>> index;

        public SecondaryIndex(IComparer<JToken> comparer, string indexDef, IPersistentSource persistentSource)
        {
            this.comparer = comparer;
            this.indexDef = indexDef;
            this.persistentSource = persistentSource;
            this.index = new SortedList<JToken, SortedSet<JToken>>(comparer);
        }

        public override string ToString()
        {
            return indexDef + " (" + index.Count + ")";
        }

        public long Count
        {
            get
            {
                lock (index)
                    return index.Count;
            }
        }

        public void Add(JToken key)
        {
            var indexOfKey = index.IndexOfKey(key);
            if (indexOfKey < 0)
            {
                index[key] = new SortedSet<JToken>(JTokenComparer.Instance)
                {
                    key
                };
            }
            else
            {
                index.Values[indexOfKey].Add(key);
            }
        }

        public void Remove(JToken key)
        {
            var indexOfKey = index.IndexOfKey(key);
            if (indexOfKey < 0)
            {
                return;
            }
            index.Values[indexOfKey].Remove(key);
            if (index.Values[indexOfKey].Count == 0)
                index.Remove(key);
        }


        public IEnumerable<JToken> SkipFromEnd(int start)
        {
            return persistentSource.Read(_ => SkipFromEndInternal(start));
        }

        private IEnumerable<JToken> SkipFromEndInternal(int start)
        {
            for (int i = (index.Count - 1) - start; i >= 0; i--)
            {
                foreach (var item in index.Values[i])
                {
                    yield return item;
                }
            }
        }

        public IEnumerable<JToken> SkipAfter(JToken key)
        {
            return Skip(key, i => i <= 0);
        }

        private IEnumerable<JToken> Skip(JToken key, Func<int, bool> shouldMoveToNext)
        {
            var recordingComparer = new RecordingComparer();
            Array.BinarySearch(index.Keys.ToArray(), key, recordingComparer);

            if (recordingComparer.LastComparedTo == null)
                yield break;

            var indexOf = index.IndexOfKey(recordingComparer.LastComparedTo);

            if (shouldMoveToNext(comparer.Compare(recordingComparer.LastComparedTo, key)))
                indexOf += 1; // skip to the next higher value

            for (int i = indexOf; i < index.Count; i++)
            {
                foreach (var item in index.Values[i])
                {
                    yield return item;
                }
            }
        }

        public IEnumerable<JToken> SkipTo(JToken key)
        {
            return persistentSource.Read(_=> Skip(key, i => i < 0));
        }

        public JToken LastOrDefault()
        {
            return persistentSource.Read(_ =>
            {
                if (index.Count == 0)
                    return null;
                return index.Keys[index.Count - 1];
            });
        }

        public JToken FirstOrDefault()
        {
            return persistentSource.Read(_ =>
            {
                if (index.Count == 0)
                    return null;
                return index.Keys[0];
            });
        }
    }
}