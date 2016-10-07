using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Sparrow;

namespace Raven.Server.Documents
{
    public class ResourceCache<TResource> : IEnumerable<KeyValuePair<StringSegment, Task<TResource>>>
    {
        readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseInsensitive = 
                    new ConcurrentDictionary<StringSegment, Task<TResource>>(CaseInsensitiveStringSegmentEqualityComparer.Instance);
        readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseSensitive = new ConcurrentDictionary<StringSegment, Task<TResource>>();

        public IEnumerable<Task<TResource>> Values => _caseSensitive.Values;

        public void Clear()
        {
            _caseSensitive.Clear();
            _caseInsensitive.Clear();
        }

        public bool TryGetValue(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (_caseSensitive.TryGetValue(resourceName, out resourceTask))
                return true;
            return _caseInsensitive.TryGetValue(resourceName, out resourceTask);
        }

        public bool TryRemove(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            Task<TResource> _;
            _caseSensitive.TryRemove(resourceName, out _);
            return _caseInsensitive.TryRemove(resourceName, out resourceTask);
        }

        public IEnumerator<KeyValuePair<StringSegment, Task<TResource>>> GetEnumerator()
        {
            return _caseInsensitive.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public Task<TResource> GetOrAdd(StringSegment databaseName, Task<TResource> task)
        {
            Task<TResource> value;
            if (_caseSensitive.TryGetValue(databaseName, out value))
                return value;

            if (_caseInsensitive.TryGetValue(databaseName, out value))
                return value;

            lock (this)
            {
                if (_caseInsensitive.TryGetValue(databaseName, out value))
                    return value;

                value = _caseInsensitive.GetOrAdd(databaseName, task);
                _caseSensitive[databaseName] = value;
                return value;
            }
        }
    }
}