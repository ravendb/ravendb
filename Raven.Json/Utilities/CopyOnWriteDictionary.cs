using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Json.Utilities
{
    public class CopyOnWriteJDictionary<TKey> : IDictionary<TKey, RavenJToken>
    {
        private static readonly RavenJToken DeletedMarker = new RavenJValue("*DeletedMarker*", JTokenType.Null);
    	private int deleteCount;

		private CopyOnWriteJDictionary<TKey> inherittedValues;
		private IDictionary<TKey, RavenJToken> localChanges;

		protected IDictionary<TKey, RavenJToken> LocalChanges
		{
			get
			{
				return localChanges ?? (localChanges = new Dictionary<TKey, RavenJToken>());
			}
		}

        public CopyOnWriteJDictionary()
        {
        }

        private CopyOnWriteJDictionary(IDictionary<TKey, RavenJToken> props)
        {
            localChanges = props;
        }

        private CopyOnWriteJDictionary(CopyOnWriteJDictionary<TKey> previous)
        {
            inherittedValues = previous;
        }

        #region Dictionary<TKey,TValue> Members

        public void Add(TKey key, RavenJToken value)
        {
			if (ContainsKey(key))
				throw new ArgumentException("An item with the same key has already been added: " + key);

        	LocalChanges[key] = value; // we can't use Add, because LocalChanges may contain a DeletedMarker
        }

    	public bool ContainsKey(TKey key)
    	{
    		RavenJToken token;
    		if (localChanges != null && localChanges.TryGetValue(key, out token))
    		{
    			if (token == DeletedMarker)
    				return false;
    			return true;
    		}
    		return (inherittedValues != null && inherittedValues.TryGetValue(key, out token) && token != DeletedMarker);
    	}

    	public ICollection<TKey> Keys
        {
			get
			{
				if (localChanges == null)
					return inherittedValues != null ? inherittedValues.Keys : new HashSet<TKey>();

				ICollection<TKey> ret = new HashSet<TKey>();
				if (inherittedValues != null)
				{
					foreach (var key in inherittedValues.Keys)
					{
						if (localChanges.ContainsKey(key))
							continue;
						ret.Add(key);
					}
				}

				foreach (var key in localChanges.Keys)
				{
					if (localChanges[key] == DeletedMarker)
						continue;
					ret.Add(key);
				}
				return ret;
			}
        }

		public bool Remove(TKey key)
		{
			RavenJToken token;
			if (!LocalChanges.TryGetValue(key, out token))
			{
				if (inherittedValues == null || !inherittedValues.TryGetValue(key, out token))
					return false;
			}

			if (token == DeletedMarker)
				return false;

			deleteCount += 1;
			LocalChanges[key] = DeletedMarker;
			return true;
		}

    	public bool TryGetValue(TKey key, out RavenJToken value)
		{
			value = null;
			RavenJToken unsafeVal;
			if (localChanges != null && localChanges.TryGetValue(key, out unsafeVal))
			{
				if (unsafeVal == DeletedMarker)
					return false;

				value = unsafeVal;
				return true;
			}

			if (inherittedValues == null || !inherittedValues.TryGetValue(key, out unsafeVal) || unsafeVal == DeletedMarker)
				return false;

			// Will also perform a copy-on-write clone on object supporting this
			// TODO: Somehow unsafeval is being assigned a null; before adding a null check we need to eliminate this within Raven
			/*var safeVal = unsafeVal.CloneToken();
			LocalChanges[key] = safeVal;
			value = safeVal;*/
    		value = unsafeVal;
			return true;
		}

    	public ICollection<RavenJToken> Values
        {
            get
            {
            	ICollection<RavenJToken> ret = new HashSet<RavenJToken>();
            	foreach (var key in Keys)
            	{
					ret.Add(this[key]);
            	}
            	return ret;
            }
        }

        public RavenJToken this[TKey key]
        {
            get
            {
            	RavenJToken token;
				if (TryGetValue(key, out token))
					return token;
            	throw new KeyNotFoundException(key.ToString());
            }
        	set
        	{
				RavenJToken token;
				if (LocalChanges.TryGetValue(key, out token) && token == DeletedMarker)
					deleteCount -= 1;
				
				LocalChanges[key] = value;
        	}
        }

        #endregion

		public class CopyOnWriteDictEnumerator : IEnumerator<KeyValuePair<TKey, RavenJToken>>
		{
			private readonly IEnumerator<KeyValuePair<TKey, RavenJToken>> _inheritted, _local;
			private IEnumerator<KeyValuePair<TKey, RavenJToken>> _current;

			public CopyOnWriteDictEnumerator(IEnumerator<KeyValuePair<TKey, RavenJToken>> inheritted, IEnumerator<KeyValuePair<TKey, RavenJToken>> local)
			{
				_inheritted = inheritted;
				_local = local;
				_current = _inheritted ?? _local;
			}

			public void Dispose()
			{
				if (_inheritted != null) _inheritted.Dispose();
				if (_local != null) _local.Dispose();
			}

			public bool MoveNext()
			{
				if (_current == null)
					return false;

				while (true)
				{
					if (!_current.MoveNext())
					{
						if (_current == _inheritted && _local != null)
						{
							_current = _local;
							continue;
						}
						_current = null;
						return false;
					}

					if (_current.Current.Value != DeletedMarker)
						return true;
				}
			}

			public void Reset()
			{
				if (_inheritted != null) _inheritted.Reset();
				if (_local != null) _local.Reset();
				_current = _inheritted ?? _local;
			}

			public KeyValuePair<TKey, RavenJToken> Current
			{
				get
				{
					if (_current == null)
						throw new InvalidOperationException();
					return _current.Current;
				}
			}

			object IEnumerator.Current
			{
				get { return Current; }
			}
		}

		public IEnumerator<KeyValuePair<TKey, RavenJToken>> GetEnumerator()
		{
			return new CopyOnWriteDictEnumerator(
				inherittedValues != null ? inherittedValues.GetEnumerator() : null,
				localChanges != null ? (IEnumerator<KeyValuePair<TKey, RavenJToken>>)new Dictionary<TKey, RavenJToken>(localChanges).GetEnumerator() : null
				);
		}

    	IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<TKey, RavenJToken> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
        	foreach (var key in Keys.ToArray()) // clone the values for the iteration
        	{
        		Remove(key);
        	}
        }

    	public bool Contains(KeyValuePair<TKey, RavenJToken> item)
        {
			throw new NotImplementedException();
		}

    	public void CopyTo(KeyValuePair<TKey, RavenJToken>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<TKey, RavenJToken> item)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return LocalChanges.Count - deleteCount + (inherittedValues != null ? inherittedValues.Count : 0); }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public CopyOnWriteJDictionary<TKey> Clone()
        {
            if (inherittedValues == null)
            {
                inherittedValues = new CopyOnWriteJDictionary<TKey>(localChanges);
                localChanges = null;
                return new CopyOnWriteJDictionary<TKey>(inherittedValues);
            }
            if (localChanges == null)
            {
                return new CopyOnWriteJDictionary<TKey>(inherittedValues);
            }
            inherittedValues = new CopyOnWriteJDictionary<TKey>(
                new CopyOnWriteJDictionary<TKey>(inherittedValues) { localChanges = localChanges });
            localChanges = null;
            return new CopyOnWriteJDictionary<TKey>(inherittedValues);
        }
    }
}
