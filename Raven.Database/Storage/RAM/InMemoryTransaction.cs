using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Raven.Database.Storage.RAM
{
	public class InMemoryTransaction
	{
		private static readonly ThreadLocal<InMemoryTransaction> current = new ThreadLocal<InMemoryTransaction>();

		public static InMemoryTransaction Current
		{
			get
			{
				if(current.Value == null)
					throw new InvalidOperationException("Transaction was not began");
				return current.Value;
			}
		}

		private class TransactionalState
		{
			public object State;
			public Action<object> OnCommit;
		}

		private readonly IDictionary<object, TransactionalState> _transactionState = new Dictionary<object, TransactionalState>();

		internal TVal GetLocalState<TVal>(object item, Func<TVal> generate, Action<TVal> onCommit)
		{
			TransactionalState val;
			if (_transactionState.TryGetValue(item, out val))
				return (TVal)val.State;

			var localState = generate();
			_transactionState.Add(item, new TransactionalState
				{
					State = localState,
					OnCommit = o => onCommit((TVal)o)
				});
			return localState;
		}

		public static void Begin()
		{
			if (current.Value != null)
			{
				// already in tx, just ignore this call;
				return;
			}
			current.Value = new InMemoryTransaction();
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public static void Commit()
		{
			foreach (var transactionalState in Current._transactionState)
			{
				transactionalState.Value.OnCommit(transactionalState.Value.State);
			}
			current.Value._transactionState.Clear();
		}

		public static void Dispose()
		{
			current.Value = null;
		}
	}
}