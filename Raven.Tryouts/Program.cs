using System;
using Raven.Tests.Bugs.DTC;
using Raven.Tests.Stress;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			new StressTester().munin_stress_testing_ravendb_100kb_in_filesystem();
		}

		private static void Do()
		{
			new UsingDTCForUpdates().can_update_a_doc_within_transaction_scope();
		}
	}
}
