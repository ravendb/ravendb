using System;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tryouts.Json;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
		    Console.WriteLine("Starting...");

		    var executeManyFileReads2 = PerfTest.ExecuteManyFileReads2(PerfTest.BigJsonPath);

		    Console.WriteLine("Ready...");

		    Console.ReadLine();

		    while (true)
		    {
		        PerfTest.CloneALot2(executeManyFileReads2);
		    }
		}
	}
}
