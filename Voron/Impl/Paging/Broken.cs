using System;
using System.Linq;
using Voron.Impl.Journal;
using Voron.Trees;

namespace Voron.Impl.Paging
{
	public class FragmentedPureMemoryPager : AbstractPager
	{
		private readonly PureMemoryJournalWriter.Buffer[] buffers;

		internal FragmentedPureMemoryPager(PureMemoryJournalWriter.Buffer[] buffers)
		{
			this.buffers = buffers;
			NumberOfAllocatedPages = buffers.Sum(x => x.Size);
		}

		public override unsafe byte* AcquirePagePointer(long pageNumber)
		{
			long page = 0;
			foreach (var buffer in buffers)
			{
				if (page == pageNumber)
					return buffer.Pointer;

				page += buffer.Size;
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