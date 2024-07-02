using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public sealed class ResourceCache<TResource> : IEnumerable<KeyValuePair<StringSegment, Task<TResource>>>
    {
        private FrozenDictionary<StringSegment, Task<TResource>> _readonlyCaseInsensitive;
        private FrozenDictionary<StringSegment, Task<TResource>> _readonlyCaseSensitive;
        private FrozenDictionary<Task<TResource>, ResourceDetails> _readonlyResourceDetails;

        private readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseInsensitive = new(StringSegmentComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<StringSegment, Task<TResource>> _caseSensitive = new (StringSegmentComparer.Ordinal);
        private readonly ConcurrentDictionary<Task<TResource>, ResourceDetails> _resourceDetails = new ();

        private readonly ConcurrentDictionary<StringSegment, ConcurrentSet<StringSegment>> _mappings = new(StringSegmentComparer.OrdinalIgnoreCase);


        public ResourceCache()
        {
            _readonlyCaseSensitive = _caseSensitive.ToFrozenDictionaryWithSameComparer();
            _readonlyCaseInsensitive = _caseInsensitive.ToFrozenDictionaryWithSameComparer();
            _readonlyResourceDetails = _resourceDetails.ToFrozenDictionaryWithSameComparer();
        }

        public sealed class ResourceDetails
        {
            public DateTime InCacheSince;
        }

        /// <summary>
        /// This locks the entire cache. Use carefully.
        /// </summary>
        public IEnumerable<Task<TResource>> Values => _readonlyCaseInsensitive.Values;

        public int Count => _readonlyCaseInsensitive.Count;

        internal int DetailsCount => _readonlyResourceDetails.Count;

        public void Clear()
        {
            _caseSensitive.Clear();
            _readonlyCaseSensitive = _caseSensitive.ToFrozenDictionaryWithSameComparer();

            _caseInsensitive.Clear();
            _readonlyCaseInsensitive = _caseInsensitive.ToFrozenDictionaryWithSameComparer();

            _resourceDetails.Clear();
            _readonlyResourceDetails = _resourceDetails.ToFrozenDictionaryWithSameComparer();
        }

        public bool TryGetValue(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (_readonlyCaseSensitive.TryGetValue(resourceName, out resourceTask))
                return true;

            return UnlikelyTryGet(resourceName, out resourceTask);
        }

        public bool TryGetValue(StringSegment resourceName, out Task<TResource> resourceTask, out ResourceDetails details)
        {
            details = null;
            if (TryGetValue(resourceName, out resourceTask) == false)
                return false;

            return _readonlyResourceDetails.TryGetValue(resourceTask, out details);
        }

        private bool UnlikelyTryGet(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (_readonlyCaseInsensitive.TryGetValue(resourceName, out resourceTask) == false)
                return false;

            _forTestingPurposes?.OnUnlikelyTryGet?.Invoke();

            lock (this)
            {
                if (_readonlyCaseInsensitive.TryGetValue(resourceName, out var resourceTaskUnderLock) == false)
                {
                    // database was deleted
                    return false;
                }

                if (resourceTask != resourceTaskUnderLock)
                {
                    // we have a case in sensitive match, but it is not the same instance
                    resourceTask = resourceTaskUnderLock;
                    return true;
                }

                //we have a case insensitive match, let us optimize that
                if (_mappings.TryGetValue(resourceName, out ConcurrentSet<StringSegment> mappingsForResource))
                {
                    mappingsForResource.Add(resourceName);
                    _caseSensitive.TryAdd(resourceName, resourceTask);
                    _readonlyCaseSensitive = _caseSensitive.ToFrozenDictionaryWithSameComparer();
                }
            }
            return true;
                
        }

        public bool TryRemove(StringSegment resourceName, Task<TResource> resourceTask)
        {
            lock (this)
            {
                if (_caseInsensitive.TryRemove(new KeyValuePair<StringSegment, Task<TResource>>(resourceName, resourceTask)) == false)
                    return false;

                _resourceDetails.Remove(resourceTask, out _);

                RemoveCaseSensitive(resourceName, resourceTask);

                // We need to refresh all of them. Since construction takes time, we will update all the references
                // at the end of the execution in order to diminish the time that an unstable state can be observed.
                // While this is not a big issue because the code already supports dealing with that, the smaller the
                // time such inconsistencies can be observed, the better.
                var readonlyCaseSensitive = _caseSensitive.ToFrozenDictionaryWithSameComparer();
                var readonlyCaseInsensitive = _caseInsensitive.ToFrozenDictionaryWithSameComparer();
                var readonlyResourceDetails = _resourceDetails.ToFrozenDictionaryWithSameComparer();

                _readonlyCaseSensitive = readonlyCaseSensitive;
                _readonlyCaseInsensitive = readonlyCaseInsensitive;
                _readonlyResourceDetails = readonlyResourceDetails;
            }

            return true;
        }

        private void RemoveCaseSensitive(StringSegment resourceName, Task<TResource> resourceTask)
        {
            Debug.Assert(Monitor.IsEntered(this));

            if (_mappings.TryGetValue(resourceName, out ConcurrentSet<StringSegment> mappings))
            {
                foreach (var mapping in mappings)
                {
                    // Careful here, the TryRemove method by its documentation would perform a (key,value) check instead of 
                    // removing the item which has a matching key. This is an important departure of usual usage and has already
                    // been the source of RavenDB-19002 
                    // https://github.com/ravendb/ravendb/commit/34c9fcb5a111b352795f30ce24bd91d7a68bfe31
                    _caseSensitive.TryRemove(new KeyValuePair<StringSegment, Task<TResource>>(mapping, resourceTask));
                }
            }
        }

        public bool TryGetAndRemove(StringSegment resourceName, out Task<TResource> resourceTask)
        {
            if (TryGetValue(resourceName, out resourceTask) == false)
                return false;

            if (TryRemove(resourceName, resourceTask) == false)
                return false;

            return true;
        }


        public IEnumerator<KeyValuePair<StringSegment, Task<TResource>>> GetEnumerator()
        {
            return _readonlyCaseInsensitive.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public Task<TResource> GetOrAdd(StringSegment databaseName, Task<TResource> task)
        {
            if (_readonlyCaseSensitive.TryGetValue(databaseName, out Task<TResource> value))
                return value;

            if (_readonlyCaseInsensitive.TryGetValue(databaseName, out value))
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
                _mappings[databaseName] = [databaseName];

                // We need to refresh all of them. Since construction takes time, we will update all the references
                // at the end of the execution in order to diminish the time that an unstable state can be observed.
                // While this is not a big issue because the code already supports dealing with that, the smaller the
                // time such inconsistencies can be observed, the better.
                var readonlyCaseSensitive = _caseSensitive.ToFrozenDictionaryWithSameComparer();
                var readonlyCaseInsensitive = _caseInsensitive.ToFrozenDictionaryWithSameComparer();
                var readonlyResourceDetails = _resourceDetails.ToFrozenDictionaryWithSameComparer();

                _readonlyCaseSensitive = readonlyCaseSensitive;
                _readonlyCaseInsensitive = readonlyCaseInsensitive;
                _readonlyResourceDetails = readonlyResourceDetails;

                return value;
            }
        }

        public IDisposable RemoveLockAndReturn(string databaseName, Action<TResource> onSuccess, out TResource resource, [CallerMemberName] string caller = null, string reason = null)
        {

            Task<TResource> current = null;
            Task<TResource> resourceLocked;

            lock (this)
            {
                var dbDisabledExMessage = $"The database '{databaseName}' has been unloaded and locked";

                if (string.IsNullOrEmpty(caller) == false)
                    dbDisabledExMessage += $" by {caller}";

                if (string.IsNullOrEmpty(reason) == false)
                    dbDisabledExMessage += $" because {reason}";

                var databaseDisabledException = new DatabaseDisabledException(dbDisabledExMessage)
                {
                    Data =
                    {
                        [DatabasesLandlord.DoNotRemove] = true,
                    }
                };

                if (caller != null)
                    databaseDisabledException.Data["Source"] = caller;

                resourceLocked = Task.FromException<TResource>(databaseDisabledException);

                resourceLocked.IgnoreUnobservedExceptions();

                bool found = false;
                while (found == false)
                {
                    found = _caseInsensitive.TryGetValue(databaseName, out current);
                    if (found == false)
                    {
                        resource = default(TResource);
                        if (_caseInsensitive.TryAdd(databaseName, resourceLocked) == false)
                            continue;

                        // We need to refresh only the case-insensitive dictionary.
                        _readonlyCaseInsensitive = _caseInsensitive.ToFrozenDictionaryWithSameComparer();

                        return new DisposableAction(() =>
                        {
                            _forTestingPurposes?.OnRemoveLockAndReturnDispose?.Invoke(this);

                            // This is a disposable action, therefore it has to execute the locking one. 
                            TryRemove(databaseName, resourceLocked);
                        });
                    }
                }

                if (current.IsCompleted == false)
                {
                    var dbConcurrentLoadTimeoutExMessage = $"Attempting to unload database {databaseName} that is loading is not allowed";

                    if (string.IsNullOrEmpty(caller) == false)
                        dbConcurrentLoadTimeoutExMessage += $" (by {caller})";

                    var databaseConcurrentLoadTimeoutException = new DatabaseConcurrentLoadTimeoutException(dbConcurrentLoadTimeoutExMessage);

                    if (string.IsNullOrEmpty(caller) == false)
                        databaseConcurrentLoadTimeoutException.Data[caller] = null;

                    throw databaseConcurrentLoadTimeoutException;
                }

                if (current.IsCompletedSuccessfully)
                {
                    _caseInsensitive.TryUpdate(databaseName, resourceLocked, current);
                    _resourceDetails.TryRemove(current, out _);
                    RemoveCaseSensitive(databaseName, current);

                    // We need to refresh all of them. Since construction takes time, we will update all the references
                    // at the end of the execution in order to diminish the time that an unstable state can be observed.
                    // While this is not a big issue because the code already supports dealing with that, the smaller the
                    // time such inconsistencies can be observed, the better.
                    var readonlyCaseSensitive = _caseSensitive.ToFrozenDictionaryWithSameComparer();
                    var readonlyCaseInsensitive = _caseInsensitive.ToFrozenDictionaryWithSameComparer();
                    var readonlyResourceDetails = _resourceDetails.ToFrozenDictionaryWithSameComparer();

                    _readonlyCaseSensitive = readonlyCaseSensitive;
                    _readonlyCaseInsensitive = readonlyCaseInsensitive;
                    _readonlyResourceDetails = readonlyResourceDetails;
                }
            }

            if (current.IsCompletedSuccessfully)
            {
                resource = current.Result; // completed, not waiting here.
                onSuccess?.Invoke(current.Result);
                return new DisposableAction(() =>
                {
                    _forTestingPurposes?.OnRemoveLockAndReturnDispose?.Invoke(this);

                    TryRemove(databaseName, resourceLocked);
                });
            }

            if (current.IsFaulted && DatabasesLandlord.IsLockedDatabase(current.Exception) == false)
            {
                // some real exception occurred, but we still want to remove / unload the faulty database
                resource = default;
                return new DisposableAction(() =>
                {
                    _forTestingPurposes?.OnRemoveLockAndReturnDispose?.Invoke(this);

                    TryRemove(databaseName, current);
                });
            }

            current.Wait();// will throw immediately because the task failed
            resource = default(TResource);
            Debug.Assert(false, "Should never reach this place");
            return null;// never used
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff(this);
        }

        internal sealed class TestingStuff
        {
            private readonly ResourceCache<TResource> _parent;

            public TestingStuff(ResourceCache<TResource> parent)
            {
                _parent = parent;
            }

            internal Action<ResourceCache<TResource>> OnRemoveLockAndReturnDispose;
            internal Action OnUnlikelyTryGet;

            public Task<TResource> Replace(string databaseName, Task<TResource> task)
            {
                lock (_parent)
                {
                    Task<TResource> existingTask = null;
                    _parent._caseInsensitive.AddOrUpdate(databaseName, segment => task, (key, existing) =>
                    {
                        existingTask = existing;
                        return task;
                    });

                    ResourceDetails details = null;
                    if (existingTask != null)
                        _parent._resourceDetails.TryRemove(existingTask, out details);

                    _parent._resourceDetails[task] = details ?? new ResourceDetails { InCacheSince = SystemTime.UtcNow };

                    if (_parent._mappings.TryGetValue(databaseName, out ConcurrentSet<StringSegment> mappings))
                    {
                        foreach (var mapping in mappings)
                        {
                            _parent._caseSensitive.TryRemove(mapping, out Task<TResource> _);
                        }
                    }

                    // We need to refresh all of them. Since construction takes time, we will update all the references
                    // at the end of the execution in order to diminish the time that an unstable state can be observed.
                    // While this is not a big issue because the code already supports dealing with that, the smaller the
                    // time such inconsistencies can be observed, the better.
                    var readonlyCaseSensitive = _parent._caseSensitive.ToFrozenDictionaryWithSameComparer();
                    var readonlyCaseInsensitive = _parent._caseInsensitive.ToFrozenDictionaryWithSameComparer();
                    var readonlyResourceDetails = _parent._resourceDetails.ToFrozenDictionaryWithSameComparer();

                    _parent._readonlyCaseSensitive = readonlyCaseSensitive;
                    _parent._readonlyCaseInsensitive = readonlyCaseInsensitive;
                    _parent._readonlyResourceDetails = readonlyResourceDetails;

                    return existingTask;
                }
            }
        }
    }
}
