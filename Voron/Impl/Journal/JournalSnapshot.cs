using System.Collections.Immutable;
using Voron.Trees;

namespace Voron.Impl.Journal
{
	public class JournalSnapshot
	{
		public long Number;
		public ImmutableDictionary<long, long> PageTranslationTable;
	}
}