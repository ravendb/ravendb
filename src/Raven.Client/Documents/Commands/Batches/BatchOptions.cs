using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Commands.Batches
{
    public class UniqueTypeSet<TValue>
    {
        private Dictionary<Type, TValue> _dictionary;

        public void AddOrUpdate<T>(T item) where T : TValue
        {
            if(_dictionary == null)
                _dictionary = new Dictionary<Type, TValue>();

            _dictionary[typeof(T)] = item;
        }

        public bool TryGetValue<T>(out T item) where T : TValue
        {
            if (_dictionary != null && _dictionary.TryGetValue(typeof(T), out var innerItem))
            {
                item = (T)innerItem;
                return true;
            }
            item = default(T);
            return false;
        }

        public IEnumerable<TValue> List()
        {
            foreach (var dictionaryKey in _dictionary.Keys)
            {
                yield return _dictionary[dictionaryKey];
            }
        }
    }

    public class BatchOptions : UniqueTypeSet<IBatchOptions>
    {
        public TimeSpan? RequestTimeout { get; set; }
    }

    public interface IBatchOptions
    {
    }

    public class ClusterBatchOptions : IBatchOptions
    {
    }

    public class IndexBatchOptions : IBatchOptions
    {
        public bool WaitForIndexes { get; set; }
        public TimeSpan WaitForIndexesTimeout { get; set; }
        public bool ThrowOnTimeoutInWaitForIndexes { get; set; }
        public string[] WaitForSpecificIndexes { get; set; }
    }

    public class ReplicationBatchOptions : IBatchOptions
    {
        public bool WaitForReplicas { get; set; }
        public int NumberOfReplicasToWaitFor { get; set; }
        public TimeSpan WaitForReplicasTimeout { get; set; }
        public bool Majority { get; set; }
        public bool ThrowOnTimeoutInWaitForReplicas { get; set; }
    }
}
