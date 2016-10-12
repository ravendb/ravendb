using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Server.Documents
{
    [SuppressMessage("ReSharper", "InconsistentlySynchronizedField")]
    public class ResourceCache<TResource> : IEnumerable<KeyValuePair<StringSegment, Task<TResource>>>
    {
        readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseInsensitive = 
                    new ConcurrentDictionary<StringSegment, Task<TResource>>(CaseInsensitiveStringSegmentEqualityComparer.Instance);
        readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseSensitive = new ConcurrentDictionary<StringSegment, Task<TResource>>();

        private readonly ConcurrentDictionary<StringSegment, ConcurrentSet<StringSegment>> _mappings =
            new ConcurrentDictionary<StringSegment, ConcurrentSet<StringSegment>>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

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
            if (_caseInsensitive.TryGetValue(resourceName, out resourceTask) == false)
                return false;

            lock (this)
            {
                //we have a case insensitive match, let us optimize that
                ConcurrentSet<StringSegment> mappingsForResource;
                if (_mappings.TryGetValue(resourceName, out mappingsForResource))
                {
                    mappingsForResource.Add(resourceName);
                    _caseSensitive.TryAdd(resourceName, resourceTask);
                }
            }
            return true;
                
        }

        public bool TryRemove(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (_caseInsensitive.TryRemove(resourceName, out resourceTask) == false)
                return false;

            lock (this)
            {
                ConcurrentSet<StringSegment> mappings;
                if (_mappings.TryGetValue(resourceName, out mappings))
                {
                    foreach (var mapping in mappings)
                    {
                        Task<TResource> _;
                        _caseSensitive.TryRemove(mapping, out _);
                    }
                }
            }

            return true;
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
                _mappings[databaseName] = new ConcurrentSet<StringSegment>
                {
                    databaseName
                };
                return value;
            }
        }
    }
}