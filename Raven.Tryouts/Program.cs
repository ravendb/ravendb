using System;
using Raven.Database.DiskIO;
using Raven.Json.Linq;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long)1024 * 1024 * 1024,
                OperationType = OperationType.Write,
                Path = "e:\\temp\\data.dat",
                Sequential = true,
                ThreadCount = 1,
                TimeToRunInSeconds = 10,
                ChunkSize = 4 * 1024
            };

		    var tester = new DiskPerformanceTester(performanceRequest, Console.WriteLine, result => Console.WriteLine("inter result"));
            tester.TestDiskIO();

		    var r = tester.Result;
            Console.WriteLine(RavenJObject.FromObject(r));
            Console.WriteLine("Total write = {0,10:#,#;;0}", r.TotalWritten);
            Console.WriteLine("Total requests (Write) = {0,10:#,#;;0}", r.WriteCount);
            Console.WriteLine("JSON: " + RavenJObject.FromObject(r.WriteMetric));
		    Console.ReadKey();
		}
	}


	
}