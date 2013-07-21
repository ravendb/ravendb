using System;
using System.IO.MemoryMappedFiles;
using Nevar.Trees;

namespace Nevar.Impl
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
		    var pos = (n*Constants.PageSize);
		    if (pos + Constants.PageSize >= _viewAccessor.Capacity)
		        throw new InvalidOperationException("Request past end of mapped memory");

		    return new Page(_baseAddress + pos);
		}

	    public Page Allocate(Transaction tx, int num)
		{
			var page = Get(tx.NextPageNumber);
			page.PageNumber = tx.NextPageNumber;
			tx.NextPageNumber += num;
			tx.DirtyPages.Add(page);

			page.Lower = (ushort)Constants.PageHeaderSize;
			page.Upper = Constants.PageSize;

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