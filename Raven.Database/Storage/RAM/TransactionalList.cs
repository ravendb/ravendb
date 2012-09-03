using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Storage.RAM
{
	public class TransactionalList<TVal> : IEnumerable<TVal>
	{
		private readonly List<TVal> _globalState = new List<TVal>();

		public class ListState
		{
			public List<TVal> Added = new List<TVal>();
			public List<TVal> Removed = new List<TVal>();
		}

		private ListState LocalState
		{
			get { return InMemoryTransaction.Current.GetLocalState(this, CreateLocalState, OnCommit); }
		}

		private void OnCommit(ListState obj)
		{
			foreach (var val in obj.Removed)
			{
				_globalState.Remove(val);
			}
			foreach (var val in obj.Added)
			{
				_globalState.Add(val);
			}
		}

		private ListState CreateLocalState()
		{
			return new ListState();
		}

		public void Add(TVal val)
		{
			LocalState.Added.Add(val);
		}

		public void Remove(TVal val)
		{
			LocalState.Removed.Add(val);
		}


		public IEnumerator<TVal> GetEnumerator()
		{
			foreach (var val in LocalState.Added)
			{
				yield return val;
			}

			foreach (var val in _globalState.Except(LocalState.Removed))
			{
				yield return val;
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}