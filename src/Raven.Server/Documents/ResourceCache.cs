using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Server.Documents
{
    [SuppressMessage("ReSharper", "InconsistentlySynchronizedField")]
    public class ResourceCache<TResource> : IEnumerable<KeyValuePair<StringSegment, Task<TResource>>>
    {
        private readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseInsensitive =
                    new ConcurrentDictionary<StringSegment, Task<TResource>>(StringSegmentComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseSensitive
            = new ConcurrentDictionary<StringSegment, Task<TResource>>(StringSegmentComparer.Ordinal);

        private readonly ConcurrentDictionary<Task<TResource>, ResourceDetails> _resourceDetails
            = new ConcurrentDictionary<Task<TResource>, ResourceDetails>();

        private readonly ConcurrentDictionary<StringSegment, ConcurrentSet<StringSegment>> _mappings =
            new ConcurrentDictionary<StringSegment, ConcurrentSet<StringSegment>>(StringSegmentComparer.OrdinalIgnoreCase);

        public class ResourceDetails
        {
            public DateTime InCacheSince;
        }

        /// <summary>
        /// This locks the entire cache. Use carefully.
        /// </summary>
        public IEnumerable<Task<TResource>> Values => _caseInsensitive.Values;

        public int Count => _caseInsensitive.Count;

        public void Clear()
        {
            _caseSensitive.Clear();
            _caseInsensitive.Clear();
            _resourceDetails.Clear();
        }

        public bool TryGetValue(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (_caseSensitive.TryGetValue(resourceName, out resourceTask))
                return true;
            
            return UnlikelyTryGet(resourceName, out resourceTask);
        }

        public bool TryGetValue(StringSegment resourceName, out Task<TResource> resourceTask, out ResourceDetails details)
        {
            details = null;
            if (TryGetValue(resourceName, out resourceTask) == false)
                return false;

            return _resourceDetails.TryGetValue(resourceTask, out details);
        }

        private bool UnlikelyTryGet(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (_caseInsensitive.TryGetValue(resourceName, out resourceTask) == false)
                return false;

            lock (this)
            {
                //we have a case insensitive match, let us optimize that
                if (_mappings.TryGetValue(resourceName, out ConcurrentSet<StringSegment> mappingsForResource))
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
            
            _resourceDetails.TryRemove(resourceTask, out _);

            lock (this)
            {
                RemoveCaseSensitive(resourceName);
            }

            return true;
        }

        private void RemoveCaseSensitive(StringSegment resourceName)
        {
            if (_mappings.TryGetValue(resourceName, out ConcurrentSet<StringSegment> mappings))
            {
                foreach (var mapping in mappings)
                {
                    _caseSensitive.TryRemove(mapping, out Task<TResource> _);
                }
            }
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
            if (_caseSensitive.TryGetValue(databaseName, out Task<TResource> value))
                return value;

            if (_caseInsensitive.TryGetValue(databaseName, out value))
                return value;

            lock (this)
            {
                if (_caseInsensitive.TryGetValue(databaseName, out value))
                    return value;

                value = _caseInsensitive.GetOrAdd(databaseName, task);
                if (value == task)
                {
                    _resourceDetails[task] = new ResourceDetails
                    {
                        InCacheSince = DateTime.UtcNow
                    };
                }
                _caseSensitive[databaseName] = value;
                _mappings[databaseName] = new ConcurrentSet<StringSegment>
                {
                    databaseName
                };
                return value;
            }
        }
        public IDisposable RemoveLockAndReturn(string databaseName, Action<TResource> onSuccess, out TResource resource, [CallerMemberName] string caller = null)
        {
            Task<TResource> current = null;
            lock (this)
            {
                var task = Task.FromException<TResource>(new DatabaseDisabledException($"The database '{databaseName}' has been unloaded and locked by {caller}")
                {
                    Data =
                    {
                        [DatabasesLandlord.DoNotRemove] = true,
                        ["Source"] = caller
                    }
                });

                task.IgnoreUnobservedExceptions();

                bool found = false;
                while (found == false)
                {
                    found = _caseInsensitive.TryGetValue(databaseName, out current);
                    if (found == false)
                    {
                        resource = default(TResource);
                        if (_caseInsensitive.TryAdd(databaseName, task) == false)
                            continue;
                        return new DisposableAction(() =>
                        {
                            TryRemove(databaseName, out _);
                        });
                    }
                }

                if (current.IsCompleted == false)
                {
                    throw new DatabaseConcurrentLoadTimeoutException($"Attempting to unload database {databaseName} that is loading is not allowed (by {caller})")
                    {
                        Data =
                        {
                            {caller, null}
                        }
                    };
                }

                if (current.IsCompletedSuccessfully)
                {
                    _caseInsensitive.TryUpdate(databaseName, task, current);
                    RemoveCaseSensitive(databaseName);
                }
            }
            if (current.IsCompletedSuccessfully)
            {
                resource = current.Result; // completed, not waiting here.
                onSuccess?.Invoke(current.Result);
                return new DisposableAction(() =>
                {
                    TryRemove(databaseName, out _);
                });
            }
            current.Wait();// will throw immediately because the task failed
            resource = default(TResource);
            Debug.Assert(false, "Should never reach this place");
            return null;// never used
        }

        public Task<TResource> Replace(string databaseName, Task<TResource> task)
        {
            lock (this)
            {
                Task<TResource> existingTask = null;
                _caseInsensitive.AddOrUpdate(databaseName, segment => task, (key, existing) =>
                {
                    existingTask = existing;
                    return task;
                });

                ResourceDetails details = null;
                if (existingTask != null)
                    _resourceDetails.TryRemove(existingTask, out details);

                _resourceDetails[task] = details ?? new ResourceDetails{InCacheSince = SystemTime.UtcNow};

                if (_mappings.TryGetValue(databaseName, out ConcurrentSet<StringSegment> mappings))
                {
                    foreach (var mapping in mappings)
                    {
                        _caseSensitive.TryRemove(mapping, out Task<TResource> _);
                    }
                }
                return existingTask;
            }
        }
    }
}
