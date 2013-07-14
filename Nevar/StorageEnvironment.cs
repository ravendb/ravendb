using System;
using System.IO.MemoryMappedFiles;

namespace Nevar
{
	public unsafe class StorageEnvironment : IDisposable
	{
		private readonly MemoryMappedFile _file;
		private readonly Pager _pager;
		private readonly SliceComparer _sliceComparer;

		public StorageEnvironment(MemoryMappedFile file)
		{
			_file = file;
			_pager = new Pager(file);
			using (var transaction = new Transaction(_pager))
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
			_file.Dispose();
		}

		public Tree Root { get; private set; }

		public Transaction NewTransaction()
		{
			return new Transaction(_pager);
		}
	}
}