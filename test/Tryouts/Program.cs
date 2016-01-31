using System;
using System.Diagnostics;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using BlittableTests.Documents;
using Raven.Server.Json;
using Sparrow;
using Voron;
using Voron.Tests.Bugs;

namespace Tryouts
{
    public class Program
    {
        public unsafe static void Main(string[] args)
        {
            new DocumentsCrud().CanQueryByPrefix();
            return;
            // var trie = Trie<int>.Build(new[]
            //{
            //     "admin/databases",
            //     "databases/*/docs",
            //     "databases",
            //     "databases/*/queries",
            //     "fs/*/stats",
            //     "databases/*/indexes/$",
            //     "fs/*/files",
            //     "admin/debug-info",
            //     "dbs",
            //     "dbs/*/docs",
            //     "dbs/*/queries"
            // }.ToDictionary(x => x, x => 1));

            // var tryMatch = trie.TryMatch("Databases/northwind/Docs");
            // if (tryMatch.Success)
            // {
            //     Console.WriteLine("Found");
            //     Console.WriteLine(tryMatch.Url.Substring(tryMatch.CaptureStart, tryMatch.CaptureLength));
            // }

            ////Console.WriteLine("start");
            ////var blittableFormatTests = new UnmanagedStreamTests();
            ////blittableFormatTests.BigAlloc();
            ////GC.Collect(2);
            ////GC.WaitForPendingFinalizers();
            ////Console.WriteLine("Done");
            ////return;

            //force loading of assemblyes
            Console.WriteLine(typeof(UnmanageJsonReaderTests));
            Console.WriteLine(typeof(BlittableJsonDocumentBuilder));
            Console.WriteLine(typeof(Hashing));
            Console.WriteLine(typeof(StorageEnvironment));

            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", 2);
            //Console.WriteLine("Really starting now...");
            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", int.MaxValue);

            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines", 2);
            Console.WriteLine("Really starting now...");
            WriteToStreamBenchmark.ManySmallDocs(@"D:\JSON\Lines", int.MaxValue);
            Console.WriteLine("done!");
        }
    }
}