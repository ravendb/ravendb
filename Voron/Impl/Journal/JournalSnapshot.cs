using Voron.Util;

namespace Voron.Impl.Journal
{
	public class JournalSnapshot
	{
		public long Number;
        public LinkedDictionary<long, JournalFile.PagePosition> PageTranslationTable;
	    public long AvailablePages;
	}
}