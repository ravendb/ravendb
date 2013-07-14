using System;
using System.IO.MemoryMappedFiles;

namespace Nevar
{
	public unsafe class Pager : IDisposable
	{
		private readonly MemoryMappedFile _mappedFile;
		private readonly MemoryMappedViewAccessor _viewAccessor;
		private readonly byte* _baseAddress = null;

		public int NextPageNumber { get; set; }

		public Pager(MemoryMappedFile mappedFile)
		{
			_mappedFile = mappedFile;

			_viewAccessor = mappedFile.CreateViewAccessor();

			_viewAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _baseAddress);
		}

		public Page Get(int n)
		{
			// TODO: handle when requesting data beyond the mapped region
			return new Page(_baseAddress + (n * Constants.PageSize));
		}

		public Page Allocate(Transaction tx, int num)
		{
			var page = Get(tx.NextPageNumber);
			page.PageNumber = tx.NextPageNumber;
			tx.NextPageNumber += num;
			tx.DirtyPages.Add(page);

			return page;
		}

		public void Dispose()
		{
			_viewAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
			_viewAccessor.Dispose();
			_mappedFile.Dispose();
		}
	}
}