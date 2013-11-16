using System.Collections.Immutable;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public class JournalSnapshot
	{
		public IVirtualPager Pager;
		public long WritePagePosition;
		public ImmutableDictionary<long, long> PageTranslationTable;
		public long Number;
		public JournalFile File;
		public long AvailablePages { get { return Pager.NumberOfAllocatedPages - WritePagePosition; }}

		public Page ReadPage(long pageNumber)
		{
			long logPageNumber;

			if (PageTranslationTable.TryGetValue(pageNumber, out logPageNumber))
				return Pager.Read(logPageNumber);

			return null;
		}
	}
}