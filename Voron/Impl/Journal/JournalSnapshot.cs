using System.Collections.Immutable;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public class JournalSnapshot
	{
		public long Number;
        public ImmutableDictionary<long, JournalFile.PagePosition> PageTranslationTable;
        public ImmutableDictionary<long, long> TransactionEndPositions;
	    public long AvailablePages;
	}
}