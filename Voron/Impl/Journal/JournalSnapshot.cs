using System.Collections.Immutable;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public class JournalSnapshot
	{
		public long Number;
        public SafeDictionary<long, JournalFile.PagePosition> PageTranslationTable;
        public SafeDictionary<long, long> TransactionEndPositions;
	    public long AvailablePages;
	}
}