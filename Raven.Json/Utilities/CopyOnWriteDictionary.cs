using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Json.Utilities
{
    public class CopyOnWriteJDictionary : IDictionary<string, RavenJToken>
    {
        private static readonly RavenJToken DeletedMarker = new RavenJValue("*DeletedMarker*", JTokenType.Null);

		private CopyOnWriteJDictionary inherittedValues;
		private IDictionary<string, RavenJToken> localChanges;

		protected IDictionary<string, RavenJToken> LocalChanges
		{
			get
			{
				return localChanges ?? (localChanges = new Dictionary<string, RavenJToken>());
			}
		}

        public CopyOnWriteJDictionary()
        {
        }

        private CopyOnWriteJDictionary(IDictionary<string, RavenJToken> props)
        {
            localChanges = props;
        }

        private CopyOnWriteJDictionary(CopyOnWriteJDictionary previous)
        {
            inherittedValues = previous;
        }

        #region Dictionary<string,TValue> Members

        public void Add(string key, RavenJToken value)
        {
			if (ContainsKey(key))
				throw new ArgumentException("An item with the same key has already been added: " + key);

        	LocalChanges[key] = value; // we can't use Add, because LocalChanges may contain a DeletedMarker
        }

    	public bool ContainsKey(string key)
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

    	public ICollection<string> Keys
        {
			get
			{
				if (localChanges == null)
					return inherittedValues != null ? inherittedValues.Keys : new HashSet<string>();

				ICollection<string> ret = new HashSet<string>();
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

		public bool Remove(string key)
		{
			RavenJToken token;
			if (!LocalChanges.TryGetValue(key, out token))
			{
				if (inherittedValues == null || !inherittedValues.TryGetValue(key, out token))
					return false;
			}

			if (token == DeletedMarker)
				return false;

			LocalChanges[key] = DeletedMarker;
			return true;
		}

    	public bool TryGetValue(string key, out RavenJToken value)
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
			if (unsafeVal == null)
				return true;

    		value = unsafeVal.MakeShallowCopy(this, key);
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

        public RavenJToken this[string key]
        {
            get
            {
            	RavenJToken token;
				if (TryGetValue(key, out token))
					return token;
            	throw new KeyNotFoundException(key);
            }
        	set
        	{
				LocalChanges[key] = value;
        	}
        }

        #endregion

		public class CopyOnWriteDictEnumerator : IEnumerator<KeyValuePair<string, RavenJToken>>
		{
			private readonly IEnumerator<KeyValuePair<string, RavenJToken>> _inheritted, _local;
			private IEnumerator<KeyValuePair<string, RavenJToken>> _current;

			public CopyOnWriteDictEnumerator(IEnumerator<KeyValuePair<string, RavenJToken>> inheritted, IEnumerator<KeyValuePair<string, RavenJToken>> local)
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

			public KeyValuePair<string, RavenJToken> Current
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

		public IEnumerator<KeyValuePair<string, RavenJToken>> GetEnumerator()
		{
			return new CopyOnWriteDictEnumerator(
				inherittedValues != null ? inherittedValues.GetEnumerator() : null,
				localChanges != null ? (IEnumerator<KeyValuePair<string, RavenJToken>>)new Dictionary<string, RavenJToken>(localChanges).GetEnumerator() : null
				);
		}

    	IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<string, RavenJToken> item)
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

    	public bool Contains(KeyValuePair<string, RavenJToken> item)
        {
			throw new NotImplementedException();
		}

    	public void CopyTo(KeyValuePair<string, RavenJToken>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<string, RavenJToken> item)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return Keys.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public CopyOnWriteJDictionary Clone()
        {
            if (inherittedValues == null)
            {
                inherittedValues = new CopyOnWriteJDictionary(localChanges);
                localChanges = null;
                return new CopyOnWriteJDictionary(inherittedValues);
            }
            if (localChanges == null)
            {
                return new CopyOnWriteJDictionary(inherittedValues);
            }
            inherittedValues = new CopyOnWriteJDictionary(
                new CopyOnWriteJDictionary(inherittedValues) { localChanges = localChanges });
            localChanges = null;
            return new CopyOnWriteJDictionary(inherittedValues);
        }
    }
}
