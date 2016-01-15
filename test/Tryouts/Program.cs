using System;
using System.Diagnostics;
using System.IO;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Sparrow;
using Voron;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new BlittableFormatTests().CheckRoundtrip(name: "BlittableTests.project.lock.json");

            return;

            ////force loading of assemblyes
            //Console.WriteLine(typeof(UnmanageJsonReaderTests));
            //Console.WriteLine(typeof(BlittableJsonWriter));
            //Console.WriteLine(typeof(Hashing));
            //Console.WriteLine(typeof(StorageEnvironment));

            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", 2);
            //Console.WriteLine("Reallying starting now...");
            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", int.MaxValue);

            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");
            //Console.WriteLine("Reallying starting now...");
            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");
            //Console.ReadLine();

        }
    }
}