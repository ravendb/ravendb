using System;
using log4net.Appender;
using Raven.Tests.Stress;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			var tester = new StressTester();
			tester.munin_stress_testing_ravendb_simple_object_in_filesystem();
		}
	}
}
