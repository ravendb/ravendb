using System;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Threading;
using Nevar.Impl;
using Nevar.Trees;
using System.Linq;

namespace Nevar
{
	public unsafe class StorageEnvironment : IDisposable
	{
		private readonly IVirtualPager _pager;
		private readonly SliceComparer _sliceComparer;

		private readonly ConcurrentDictionary<long, Transaction> _activeTransactions = new ConcurrentDictionary<long, Transaction>();

		private long _transactionsCounter;
		public int NextPageNumber { get; set; }

		public StorageEnvironment(IVirtualPager pager)
		{
			_pager = pager;
			using (var transaction = new Transaction(_pager, this, _transactionsCounter + 1))
			{
				_sliceComparer = NativeMethods.memcmp;
				FreeSpace = Tree.Create(transaction, _sliceComparer);
				Root = Tree.Create(transaction, _sliceComparer);
				transaction.Commit();
			}
		}

		public SliceComparer SliceComparer
		{
			get { return _sliceComparer; }
		}

		public void Dispose()
		{
			_pager.Dispose();
		}

		public Tree Root { get; private set; }
		public Tree FreeSpace { get; private set; }

		public Transaction NewTransaction()
		{
			var txId = Interlocked.Increment(ref _transactionsCounter);
			var newTransaction = new Transaction(_pager, this, txId);
			_activeTransactions.TryAdd(txId, newTransaction);
			return newTransaction;
		}

		public long OldestTransaction
		{
			get { return _activeTransactions.Keys.OrderBy(x => x).FirstOrDefault(); }
		}

		internal void TransactionCompleted(long txId)
		{
			Transaction value;
			_activeTransactions.TryRemove(txId, out value);
		}
	}
}