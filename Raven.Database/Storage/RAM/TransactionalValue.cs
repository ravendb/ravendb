using System;
using System.Data;

namespace Raven.Database.Storage.RAM
{
	public class TransactionalValue<T>
		where T : new()
	{
		private Wrapper _globalState = new Wrapper();

		private class Wrapper
		{
			public Guid Etag = Guid.NewGuid();
			public T Value;
			public bool HasValue;
		}

		private Wrapper LocalState
		{
			get { return InMemoryTransaction.Current.GetLocalState(this, CreateLocalState, OnCommit); }
		}

		private void OnCommit(Wrapper obj)
		{
			if(_globalState.Etag != obj.Etag)
				throw new DBConcurrencyException("Value has been modified by an external transaction");

			_globalState = new Wrapper
				{
					Value = obj.Value,
					HasValue = true
				};
		}

		public T Value
		{
			get
			{
				if (LocalState.HasValue)
					return LocalState.Value;
				return _globalState.Value;
			}
			set
			{
				LocalState.HasValue = true;
				LocalState.Value = value;
				LocalState.Etag = _globalState.Etag;
			}
		}

		private Wrapper CreateLocalState()
		{
			return new Wrapper();
		}
	}
}