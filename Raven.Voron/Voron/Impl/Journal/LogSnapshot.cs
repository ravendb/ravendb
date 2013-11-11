using System.Collections.Immutable;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public class LogSnapshot
	{
		public JournalFile File;

		public ImmutableDictionary<long, long> PageTranslations;

		public Page ReadPage(long pageNumber)
		{
			// here we need to do necessary translation between a reading page number and it's number in the log file
			long logPage;

			if (PageTranslations.TryGetValue(pageNumber, out logPage))
				return File.Pager.Read(logPage);

			return null;
		}
	}
}