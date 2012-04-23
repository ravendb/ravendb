using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Json.Linq
{
	internal class DictionaryWithParentSnapshot : IDictionary<string, RavenJToken>
	{
		private readonly IEqualityComparer<string> comparer;
		private static readonly RavenJToken DeletedMarker = new RavenJValue("*DeletedMarker*", JTokenType.Null);

		private readonly DictionaryWithParentSnapshot parentSnapshot;
		private bool isSnapshot;
		private int count = -1;

		protected IDictionary<string, RavenJToken> LocalChanges { get; private set; }

		public DictionaryWithParentSnapshot(IEqualityComparer<string> comparer)
		{
			this.comparer = comparer;
			LocalChanges = new Dictionary<string, RavenJToken>(comparer);
		}

		private DictionaryWithParentSnapshot(DictionaryWithParentSnapshot previous)
		{
			LocalChanges = new Dictionary<string, RavenJToken>(previous.comparer);
			parentSnapshot = previous;
		}

		#region Dictionary<string,TValue> Members

		public void Add(string key, RavenJToken value)
		{
			if (isSnapshot)
				throw new InvalidOperationException("Cannot modify a snapshot, this is probably a bug");

			if (ContainsKey(key))
				throw new ArgumentException(string.Format("An item with the same key has already been added: '{0}'", key));

			count = -1;
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
				{
					if (parentSnapshot != null)
					{
						if (count == -1) count = parentSnapshot.count;
						return parentSnapshot.Keys;
					}
					return new HashSet<string>();
				}

				int counter = 0;
				ICollection<string> ret = new HashSet<string>();
				if (parentSnapshot != null)
				{
					foreach (var key in parentSnapshot.Keys)
					{
						if (LocalChanges.ContainsKey(key))
							continue;
						ret.Add(key);
						++counter;
					}
				}

				foreach (var key in LocalChanges.Keys)
				{
					if (LocalChanges[key] == DeletedMarker)
						continue;
					ret.Add(key);
					++counter;
				}

				count = counter;
				return ret;
			}
		}

		public bool Remove(string key)
		{
			if (isSnapshot)
				throw new InvalidOperationException("Cannot modify a snapshot, this is probably a bug");

			count = -1;
			RavenJToken token;
			if (!LocalChanges.TryGetValue(key, out token))
			{
				bool parentHasIt = parentSnapshot == null || parentSnapshot.TryGetValue(key, out token);
				if (parentHasIt == false)
					return false;

				if (token == DeletedMarker)
					return false;

				LocalChanges[key] = DeletedMarker;
				return true;
			}

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
				count = -1;
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
			// we either already have count set to -1, or it will be invalidated by a call to Remove below

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
			if(parentSnapshot != null)
			{
				parentSnapshot.CopyTo(array, arrayIndex);
				arrayIndex += parentSnapshot.Count;
			}
			LocalChanges.CopyTo(array, arrayIndex);
		}

		public bool Remove(KeyValuePair<string, RavenJToken> item)
		{
			throw new NotImplementedException();
		}

		public int Count
		{
			get { return (count >= 0) ? count : Keys.Count; }
		}

		public bool IsReadOnly
		{
			get { return false; }
		}

		public bool CaseInsensitivePropertyNames { get; set; }

		public DictionaryWithParentSnapshot CreateSnapshot()
		{
			if(isSnapshot == false)
				throw new InvalidOperationException("Cannot create snapshot without previously calling EnsureSnapShot");
			return new DictionaryWithParentSnapshot(this);
		}

		public void EnsureSnapshot()
		{
			isSnapshot = true;
		}
	}
}
