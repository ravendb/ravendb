using System.IO.MemoryMappedFiles;
using Nevar.Trees;

namespace Nevar.Impl
{
	public unsafe class Pager : IVirtualPager
	{
		private readonly MemoryMappedFile _mappedFile;
		private readonly MemoryMappedViewAccessor _viewAccessor;
		private readonly byte* _baseAddress = null;

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

		public Page Allocate(int nextPageNumber, int num)
		{
			var page = Get(nextPageNumber);
			page.PageNumber = nextPageNumber;

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