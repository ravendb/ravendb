using System;
using Raven.Client.Document;
using Raven.Tests.Bugs.DTC;
using Raven.Tests.Stress;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			new StressTester().esent_stress_testing_ravendb_100kb_in_filesystem_with_indexing_case2();
		}
	}
}
