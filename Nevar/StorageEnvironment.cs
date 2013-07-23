using System;
using System.IO.MemoryMappedFiles;
using Nevar.Impl;
using Nevar.Trees;

namespace Nevar
{
	public unsafe class StorageEnvironment : IDisposable
	{
		private readonly IVirtualPager _pager;
		private readonly SliceComparer _sliceComparer;

		public int NextPageNumber { get; set; }

		public StorageEnvironment(IVirtualPager pager)
		{
			_pager = pager;
			using (var transaction = new Transaction(_pager, this))
			{
				_sliceComparer = NativeMethods.memcmp;
				Root = Tree.CreateOrOpen(transaction, -1, _sliceComparer);
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

		public Transaction NewTransaction()
		{
			return new Transaction(_pager, this);
		}
	}
}