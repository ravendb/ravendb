using System.Collections.Generic;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public class JournalSnapshot
	{
		public long Number;
		public PageTable PageTranslationTable;
	    public long AvailablePages;
		public long LastTransaction;
	}
}