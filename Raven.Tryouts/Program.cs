using System;
using Raven.Database.DiskIO;
using Raven.Json.Linq;
using System.Linq;
using Raven.Database.Extensions;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long) 1024 * 1024 * 1024,
                OperationType = OperationType.Write,
                Path = "c:\\temp\\data.ravendb-io-test",
                Sequential = true,
                ThreadCount = 4,
                TimeToRunInSeconds = 10,
                ChunkSize = 4 * 1024
            };

		    var tester = new DiskPerformanceTester(performanceRequest, Console.WriteLine);
            tester.TestDiskIO();

		    var r = tester.Result;

            Console.WriteLine(RavenJObject.FromObject(r));
		    Console.ReadKey();
		}
	}


	
}