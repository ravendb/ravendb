using System;
using System.Linq;
using Voron.Impl.Journal;
using Voron.Trees;

namespace Voron.Impl.Paging
{
	public class FragmentedPureMemoryPager : AbstractPager
	{
		private readonly PureMemoryJournalWriter.Buffer[] _buffers;

		internal FragmentedPureMemoryPager(PureMemoryJournalWriter.Buffer[] buffers)
		{
			this._buffers = buffers;
			NumberOfAllocatedPages = buffers.Sum(x => x.SizeInPages);
		}

		public override unsafe string Source
		{
			get { return "FragmentedPureMemoryPager"; }
		}

		public override unsafe byte* AcquirePagePointer(long pageNumber)
		{
			long page = 0;
			foreach (var buffer in _buffers)
			{
			    if (page + buffer.SizeInPages > pageNumber)
			        return buffer.Pointer + ((pageNumber - page)*PageSize);

				page += buffer.SizeInPages;
			}
			throw new InvalidOperationException("Could not find a matchin page number: " + pageNumber);
		}

		public override unsafe void AllocateMorePages(Transaction tx, long newLength)
		{
			throw new NotSupportedException();
		}

		public override unsafe void Sync()
		{
			throw new NotSupportedException();
		}

		public override unsafe void Write(Page page, long? pageNumber)
		{
			throw new NotSupportedException();
		}

		public override unsafe void WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			throw new NotSupportedException();
		}

		public override unsafe string ToString()
		{
			return "memory";
		}
	}
}