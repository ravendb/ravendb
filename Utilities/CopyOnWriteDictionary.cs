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
        private static readonly RavenJToken DeletedMarker = new RavenJValue(null, JTokenType.Null);
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
        	RavenJToken token;
        	if (LocalChanges.TryGetValue(key, out token) && token == DeletedMarker)
        		deleteCount -= 1;

        	LocalChanges.Add(key, value);
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

				if (localChanges != null)
				{
					foreach (var key in localChanges.Keys)
					{
						if (localChanges[key] == DeletedMarker)
							continue;
						ret.Add(key);
					}
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
				return unsafeVal != DeletedMarker;

			if (inherittedValues == null || !inherittedValues.TryGetValue(key, out unsafeVal) || unsafeVal == DeletedMarker)
				return false;

			// Will also perform a copy-on-write clone on object supporting this
			// TODO: Somehow unsafeval is being assigned a null; before adding a null check we need to eliminate this within Raven
			var safeVal = unsafeVal.CloneToken();
			LocalChanges[key] = safeVal;
			value = safeVal;
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

        public IEnumerator<KeyValuePair<TKey, RavenJToken>> GetEnumerator()
        {
        	return Keys.Select(key => new KeyValuePair<TKey, RavenJToken>(key, this[key])).GetEnumerator();
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
