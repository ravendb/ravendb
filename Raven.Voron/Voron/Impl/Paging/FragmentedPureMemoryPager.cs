using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Impl.Journal;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Paging
{
	public class FragmentedPureMemoryPager : AbstractPager
	{
		private readonly ImmutableAppendOnlyList<PureMemoryJournalWriter.Buffer> _buffers;


		internal FragmentedPureMemoryPager(ImmutableAppendOnlyList<PureMemoryJournalWriter.Buffer> buffers)
		{
			_buffers = buffers;
			NumberOfAllocatedPages = buffers.Sum(x => x.SizeInPages);
		}

		protected override string GetSourceName()
		{
			return "FragmentedPureMemoryPager";
		}

		public override unsafe byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
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

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			throw new NotSupportedException();
		}

		public override void Sync()
		{
			throw new NotSupportedException();
		}

		public override int Write(Page page, long? pageNumber)
		{
			throw new NotSupportedException();
		}

		public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			throw new NotSupportedException();
		}

		public override string ToString()
		{
			return "memory";
		}
	}
}
