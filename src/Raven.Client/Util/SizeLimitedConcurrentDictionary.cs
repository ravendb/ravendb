using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Database.Util
{
    public class SizeLimitedConcurrentDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        private readonly ConcurrentDictionary<TKey, TValue> dic;
        private readonly ConcurrentQueue<TKey> queue = new ConcurrentQueue<TKey>();

        private readonly int size;

        public SizeLimitedConcurrentDictionary(int size = 100)
            : this(size, EqualityComparer<TKey>.Default)
        {

        }

        public SizeLimitedConcurrentDictionary(int size, IEqualityComparer<TKey> equalityComparer)
        {
            this.size = size;
            dic = new ConcurrentDictionary<TKey, TValue>(equalityComparer);
        }

        public void Set(TKey key, TValue item)
        {
            dic.AddOrUpdate(key, _ => item, (_,__) => item);
            queue.Enqueue(key);

            while (queue.Count > size)
            {
                TKey result;
                if (queue.TryDequeue(out result) == false)
                    break;
                TValue value;
                dic.TryRemove(key, out value);
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return dic.TryGetValue(key, out value);
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            return dic.TryRemove(key, out value);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return dic.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
