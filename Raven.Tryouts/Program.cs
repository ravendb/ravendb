using System;
using System.Diagnostics;
using Raven.Client.Document;
using Raven.Tests.Bugs.DTC;
using Raven.Tests.Stress;
using Xunit;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			try
			{
				new StressTester().munin_stress_testing_ravendb_100kb_in_filesystem();
			}
			catch (Exception e)
			{
				Console.WriteLine(GC.GetTotalMemory(false));
				Debugger.Launch();
				Console.WriteLine(e);
			}
			//foreach (var method in typeof(StressTester).GetMethods())
			//{
			//    if (method.DeclaringType != typeof(StressTester))
			//        continue;

			//    var stressTester = new StressTester();
			//    Console.Write("Executing: " + method.Name);
			//    var sp = Stopwatch.StartNew();
			//    method.Invoke(stressTester, null);
			//    Console.WriteLine(", took " + sp.ElapsedMilliseconds);
			//}
		}
	}
}
