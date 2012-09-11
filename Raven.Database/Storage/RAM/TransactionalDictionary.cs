using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Raven.Database.Storage.RAM
{
	public class TransactionalDictionary<TKey, TVal> : IEnumerable<KeyValuePair<TKey, TVal>>
	{
		private readonly IEqualityComparer<TKey> _comparer;
		private readonly Func<TVal> generateNew;

		private class Wrapper
		{
			public Guid Etag = Guid.NewGuid();
			public TVal Value;

			public bool Deleted;
		}
		private readonly Dictionary<TKey, Wrapper> _globalState;

		public int Count
		{
			get { return _globalState.Count + LocalState.Sum(x => x.Value.Deleted ? -1 : 1); }
		}

		public TransactionalDictionary(IEqualityComparer<TKey> comparer, Func<TVal> generateNew = null)
		{
			_comparer = comparer;
			this.generateNew = generateNew;
			_globalState = new Dictionary<TKey, Wrapper>(comparer);
		}

		public TVal GetOrAdd(TKey key)
		{
			Wrapper value;
			if (LocalState.TryGetValue(key, out value))
				return value.Value;

			if (_globalState.TryGetValue(key, out value))
				return value.Value;
			if(generateNew == null)
				throw new InvalidOperationException("Cannot create a new instance without a non null generateNew pass in ctor");
			var val = generateNew();
			LocalState.Add(key, new Wrapper
				{
					Etag = Guid.Empty,
					Value = val
				});
			return val;
		}

		public TVal GetOrDefault(TKey key)
		{
			Wrapper value;
			if (LocalState.TryGetValue(key, out value))
				return value.Value;

			if (_globalState.TryGetValue(key, out value))
				return value.Value;

			return default(TVal);
		}

		private Dictionary<TKey, Wrapper> LocalState
		{
			get { return InMemoryTransaction.Current.GetLocalState(this, CreateLocalState, OnCommit); }
		}

		public void Remove(TKey key)
		{
			if (_globalState.ContainsKey(key) == false)
			{
				LocalState.Remove(key); // just need to remove from local state, is all
				return ;
			}
		
			LocalState[key] = new Wrapper
				{
					Deleted = true
				};
		}

		private void OnCommit(Dictionary<TKey, Wrapper> dictionary)
		{
			foreach (var wrapper in dictionary)
			{
				Wrapper value;
				if (_globalState.TryGetValue(wrapper.Key, out value) && wrapper.Value.Etag != value.Etag)
					throw new DBConcurrencyException("The value for " + wrapper.Key + " was modified by another transaction");
				if (wrapper.Value.Etag != Guid.Empty)
					throw new DBConcurrencyException("The value for " + wrapper.Key + " was modified by another transaction");
				
				if (wrapper.Value.Deleted)
				{
					_globalState.Remove(wrapper.Key);
				}
				else
				{
					_globalState.Add(wrapper.Key, new Wrapper
						{
							Value = wrapper.Value.Value
						});
				}
			}
		}

		private Dictionary<TKey, Wrapper> CreateLocalState()
		{
			return new Dictionary<TKey, Wrapper>(_comparer);
		}

		public void Set(TKey key, TVal val)
		{
			Wrapper value;
			if(LocalState.TryGetValue(key, out value))
			{
				value.Deleted = false;
				value.Value = val;
				return;
			}
			if(_globalState.TryGetValue(key, out value))
			{
				LocalState.Add(key, new Wrapper
				{
					Etag = value.Etag,
					Value = val
				});
				return;
			}
			LocalState.Add(key, new Wrapper
			{
				Etag = Guid.Empty,
				Value = val
			});
		}

		public IEnumerator<KeyValuePair<TKey, TVal>> GetEnumerator()
		{
			foreach (var wrapper in LocalState)
			{
				if(wrapper.Value.Deleted == false)
					yield return new KeyValuePair<TKey, TVal>(wrapper.Key, wrapper.Value.Value);
			}

			foreach (var wrapper in _globalState)
			{
				if(LocalState.ContainsKey(wrapper.Key))
					continue;
				yield return new KeyValuePair<TKey, TVal>(wrapper.Key, wrapper.Value.Value);
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}