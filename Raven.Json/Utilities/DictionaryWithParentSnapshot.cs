using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Json.Utilities
{
    public class DictionaryWithParentSnapshot : IDictionary<string, RavenJToken>
    {
        private static readonly RavenJToken DeletedMarker = new RavenJValue("*DeletedMarker*", JTokenType.Null);

		private readonly DictionaryWithParentSnapshot parentSnapshot;
        private bool isSnapshot;

        protected IDictionary<string, RavenJToken> LocalChanges { get; private set; }

        public DictionaryWithParentSnapshot()
        {
            LocalChanges = new Dictionary<string, RavenJToken>();
        }

        private DictionaryWithParentSnapshot(IDictionary<string, RavenJToken> props)
        {
            LocalChanges = props;
        }

        private DictionaryWithParentSnapshot(DictionaryWithParentSnapshot previous)
        {
            LocalChanges = new Dictionary<string, RavenJToken>();
            parentSnapshot = previous;
        }

        #region Dictionary<string,TValue> Members

        public void Add(string key, RavenJToken value)
        {
            if (isSnapshot)
                throw new InvalidOperationException("Cannot modify a snapshot, this is probably a bug");

			if (ContainsKey(key))
				throw new ArgumentException("An item with the same key has already been added: " + key);

            LocalChanges[key] = value; // we can't use Add, because LocalChanges may contain a DeletedMarker
        }

    	public bool ContainsKey(string key)
    	{
    		RavenJToken token;
    		if (LocalChanges != null && LocalChanges.TryGetValue(key, out token))
    		{
    			if (token == DeletedMarker)
    				return false;
    			return true;
    		}
    		return (parentSnapshot != null && parentSnapshot.TryGetValue(key, out token) && token != DeletedMarker);
    	}

    	public ICollection<string> Keys
        {
			get
			{
				if (LocalChanges == null)
					return parentSnapshot != null ? parentSnapshot.Keys : new HashSet<string>();

				ICollection<string> ret = new HashSet<string>();
				if (parentSnapshot != null)
				{
					foreach (var key in parentSnapshot.Keys)
					{
						if (LocalChanges.ContainsKey(key))
							continue;
						ret.Add(key);
					}
				}

				foreach (var key in LocalChanges.Keys)
				{
					if (LocalChanges[key] == DeletedMarker)
						continue;
					ret.Add(key);
				}
				return ret;
			}
        }

		public bool Remove(string key)
		{
            if (isSnapshot)
                throw new InvalidOperationException("Cannot modify a snapshot, this is probably a bug");

		    bool parentHasIt = false;
			RavenJToken token;
			if (!LocalChanges.TryGetValue(key, out token))
			{
			    parentHasIt = parentSnapshot == null || !parentSnapshot.TryGetValue(key, out token);
				if (parentHasIt == false)
					return false;
			}

			if (token == DeletedMarker)
				return false;

            if(parentHasIt)
			    LocalChanges[key] = DeletedMarker;

		    return LocalChanges.Remove(key);
		}

    	public bool TryGetValue(string key, out RavenJToken value)
		{
			value = null;
			RavenJToken unsafeVal;
			if (LocalChanges != null && LocalChanges.TryGetValue(key, out unsafeVal))
			{
				if (unsafeVal == DeletedMarker)
					return false;

				value = unsafeVal;
				return true;
			}

			if (parentSnapshot == null || !parentSnapshot.TryGetValue(key, out unsafeVal) || unsafeVal == DeletedMarker)
				return false;

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

		public IEnumerator<KeyValuePair<string, RavenJToken>> GetEnumerator()
		{
            if(parentSnapshot != null)
            {
                foreach (var item in parentSnapshot)
                {
                    if(LocalChanges.ContainsKey(item.Key))
                        continue;
                    yield return item;
                }
            }
		    foreach (var localChange in LocalChanges)
		    {
                if(localChange.Value == DeletedMarker)
                    continue;
		        yield return localChange;
		    }
		    
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

        public DictionaryWithParentSnapshot CreateSnapshot()
        {
            isSnapshot = true;
            return new DictionaryWithParentSnapshot(this);
        }
    }
}
